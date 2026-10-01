package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gatellm/internal/cache"
	"gatellm/internal/embed"
	"gatellm/internal/provider"
	"gatellm/internal/store"
)

// memSemantic is an in-memory semantic cache. It does the same maths as pgvector:
// the vectors have length 1, so similarity = dot product.
type memSemantic struct {
	mu        sync.Mutex
	rows      []semRow
	threshold float64
	findErr   error
	addErr    error
	finds     int
	adds      int
}

type semRow struct {
	tenant, scope string
	vec           []float32
	entry         *cache.Entry
}

func (m *memSemantic) Find(ctx context.Context, tenantID, scope string, vec []float32) (*cache.Entry, float64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.finds++
	if m.findErr != nil {
		return nil, 0, false, m.findErr
	}
	best, bestSim := (*cache.Entry)(nil), -1.0
	for _, r := range m.rows {
		if r.tenant != tenantID || r.scope != scope {
			continue
		}
		var sim float64
		for i := range vec {
			sim += float64(vec[i]) * float64(r.vec[i])
		}
		if sim > bestSim {
			best, bestSim = r.entry, sim
		}
	}
	if best == nil || bestSim < m.threshold {
		return nil, bestSim, false, nil
	}
	return best, bestSim, true, nil
}

func (m *memSemantic) Add(ctx context.Context, tenantID, scope, promptText string, vec []float32, e *cache.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.adds++
	if m.addErr != nil {
		return m.addErr
	}
	m.rows = append(m.rows, semRow{tenantID, scope, vec, e})
	return nil
}

// countingEmbedder wraps the mock embedder and counts calls.
type countingEmbedder struct {
	inner *embed.Mock
	calls atomic.Int64
	err   error
}

func (c *countingEmbedder) Dimensions() int { return c.inner.Dimensions() }
func (c *countingEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	c.calls.Add(1)
	if c.err != nil {
		return nil, c.err
	}
	return c.inner.Embed(ctx, text)
}

func newSemanticRig() (*cacheRig, *memSemantic, *countingEmbedder) {
	rig := newCacheRig(false)
	sem := &memSemantic{threshold: 0.8}
	emb := &countingEmbedder{inner: embed.NewMock(768)}
	// The Handler is inside the router, so build a new one that shares the fakes of the rig.
	h := &Handler{
		Providers: provider.Registry{"mock": rig.prov}, Tenants: rig.store, AdminToken: testAdminToken,
		Limiter: rig.lim, Budget: rig.bud, Cache: rig.cache, Semantic: sem, Embedder: emb,
	}
	rig.srv = NewRouter(h, 1<<20)
	return rig, sem, emb
}

func body(q string) string {
	return `{"model":"mock","temperature":0,"messages":[{"role":"user","content":"` + q + `"}]}`
}

func TestParaphraseGetsASemanticHit(t *testing.T) {
	rig, sem, _ := newSemanticRig()

	first := rig.post(body("What is the capital of France"))
	if first.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("first: %q", first.Header().Get("X-Cache"))
	}
	if sem.adds != 1 {
		t.Fatalf("the fresh answer must be stored semantically, adds=%d", sem.adds)
	}
	costBefore, deltasBefore := len(rig.bud.added), len(rig.lim.deltas)

	second := rig.post(body("What is the capital of France please"))
	if second.Code != 200 || second.Header().Get("X-Cache") != "HIT-SEMANTIC" {
		t.Fatalf("second: status %d, X-Cache %q, body %s", second.Code, second.Header().Get("X-Cache"), second.Body.String())
	}
	sim := second.Header().Get("X-Cache-Similarity")
	if sim == "" || sim < "0.8" {
		t.Errorf("X-Cache-Similarity should show the score, got %q", sim)
	}
	if rig.prov.calls.Load() != 1 {
		t.Errorf("the provider must be called once, got %d", rig.prov.calls.Load())
	}
	if len(rig.bud.added) != costBefore {
		t.Error("a semantic hit costs nothing")
	}
	if len(rig.lim.deltas) != deltasBefore+1 || rig.lim.deltas[len(rig.lim.deltas)-1] != -rig.lim.lastCost {
		t.Errorf("the reserved tokens must come back, deltas %v", rig.lim.deltas)
	}
	if second.Header().Get("X-Provider") != "mock" {
		t.Error("X-Provider names who really answered")
	}

	// The new wording was also saved in the exact cache: the next identical call is HIT-EXACT.
	third := rig.post(body("What is the capital of France please"))
	if third.Header().Get("X-Cache") != "HIT-EXACT" {
		t.Errorf("third: want HIT-EXACT, got %q", third.Header().Get("X-Cache"))
	}
}

func TestUnrelatedQuestionIsAMissAndStored(t *testing.T) {
	rig, sem, _ := newSemanticRig()
	rig.post(body("What is the capital of France"))
	rec := rig.post(body("How do I bake sourdough bread at home"))
	if rec.Header().Get("X-Cache") != "MISS" || rig.prov.calls.Load() != 2 {
		t.Errorf("X-Cache %q, provider calls %d", rec.Header().Get("X-Cache"), rig.prov.calls.Load())
	}
	if sem.adds != 2 {
		t.Errorf("both answers are stored, adds=%d", sem.adds)
	}
}

func TestSemanticNeverCrossesTenantsOrSettings(t *testing.T) {
	rig, _, _ := newSemanticRig()
	rig.store.tenants["tenant-b"] = &store.Tenant{ID: "tenant-b", Name: "b", RPMLimit: 60, TPMLimit: 20000, MonthlyBudgetUSD: 5}
	rig.store.keys[hashKey("gk_key_b")] = "tenant-b"
	rig.post(body("What is the capital of France"))

	b := do(rig.srv, http.MethodPost, "/v1/chat/completions", body("What is the capital of France please"), "Bearer gk_key_b")
	if b.Header().Get("X-Cache") != "MISS" {
		t.Errorf("another tenant must not get a semantic hit, got %q", b.Header().Get("X-Cache"))
	}

	// Same tenant, same question, but a different max_tokens: a different answer is expected.
	other := rig.post(`{"model":"mock","temperature":0,"max_tokens":5,"messages":[{"role":"user","content":"What is the capital of France please"}]}`)
	if other.Header().Get("X-Cache") != "MISS" {
		t.Errorf("a different max_tokens must not share a semantic answer, got %q", other.Header().Get("X-Cache"))
	}
}

func TestEmbeddingFailureStillAnswers(t *testing.T) {
	rig, sem, emb := newSemanticRig()
	emb.err = errors.New("embedding provider is down")

	rec := rig.post(body("What is the capital of France"))
	if rec.Code != 200 || rec.Header().Get("X-Cache") != "MISS" || !strings.Contains(rec.Body.String(), "the answer") {
		t.Fatalf("status %d, X-Cache %q", rec.Code, rec.Header().Get("X-Cache"))
	}
	if sem.finds != 0 || sem.adds != 0 {
		t.Error("without a vector the semantic cache is not used at all")
	}
	if _, sets, _ := rig.cache.counts(); sets != 1 {
		t.Error("the exact cache still works")
	}
}

func TestSemanticDatabaseErrorsNeverBreakTheRequest(t *testing.T) {
	rig, sem, _ := newSemanticRig()
	sem.findErr = errors.New("postgres is down")
	sem.addErr = errors.New("postgres is down")
	rec := rig.post(body("What is the capital of France"))
	if rec.Code != 200 || rec.Header().Get("X-Cache") != "MISS" {
		t.Errorf("status %d, X-Cache %q", rec.Code, rec.Header().Get("X-Cache"))
	}
}

func TestLongPromptSkipsTheSemanticCache(t *testing.T) {
	rig, sem, emb := newSemanticRig()
	rec := rig.post(body(strings.Repeat("a", cache.MaxSemanticChars+10)))
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	if emb.calls.Load() != 0 || sem.finds != 0 || sem.adds != 0 {
		t.Error("a very long prompt must not be embedded")
	}
}

func TestBypassedRequestsAreNeverEmbedded(t *testing.T) {
	rig, _, emb := newSemanticRig()
	rig.post(temp7Body)
	rig.post(streamTmp)
	if emb.calls.Load() != 0 {
		t.Error("requests that skip the cache must not cost an embedding call")
	}
}

func TestExactHitDoesNotCallTheEmbedder(t *testing.T) {
	rig, _, emb := newSemanticRig()
	rig.post(temp0Body)
	before := emb.calls.Load()
	rig.post(temp0Body)
	if emb.calls.Load() != before {
		t.Error("an exact hit is answered before any embedding is made")
	}
}

// Ten identical requests: one provider call AND one embedding call.
func TestOnlyTheLeaderEmbeds(t *testing.T) {
	rig, sem, emb := newSemanticRig()
	rig.prov.gate = make(chan struct{})

	done := make(chan struct{})
	go func() { fire(rig.srv, 10, temp0Body); close(done) }()
	waitStarted(t, rig.prov)
	time.Sleep(200 * time.Millisecond)
	close(rig.prov.gate)
	<-done

	if emb.calls.Load() != 1 || rig.prov.calls.Load() != 1 {
		t.Errorf("embeddings %d, provider calls %d: both must be 1", emb.calls.Load(), rig.prov.calls.Load())
	}
	if sem.finds != 1 || sem.adds != 1 {
		t.Errorf("finds %d, adds %d: both must be 1", sem.finds, sem.adds)
	}
}
