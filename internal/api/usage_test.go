package api

import (
	"bufio"
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gatellm/internal/embed"
	"gatellm/internal/provider"
	"gatellm/internal/ratelimit"
	"gatellm/internal/usage"
)

// fakeUsage remembers the events it got.
type fakeUsage struct {
	mu     sync.Mutex
	events []usage.Event
}

func (f *fakeUsage) Record(ctx context.Context, e usage.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
}

func (f *fakeUsage) all() []usage.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]usage.Event(nil), f.events...)
}

// one returns the only event, or fails the test.
func (f *fakeUsage) one(t *testing.T) usage.Event {
	t.Helper()
	ev := f.all()
	if len(ev) != 1 {
		t.Fatalf("expected exactly 1 usage event, got %d: %+v", len(ev), ev)
	}
	return ev[0]
}

func plainServer(fu *fakeUsage, p provider.Provider, withCache bool) http.Handler {
	h := &Handler{
		Providers: provider.Registry{"mock": p}, Tenants: newFakeStore(), AdminToken: testAdminToken,
		Usage: fu, Budget: &fakeBudget{status: budgetOK()},
	}
	if withCache {
		h.Cache = newMemCache()
	}
	return NewRouter(h, 1<<20)
}

func TestSuccessfulRequestRecordsOneEvent(t *testing.T) {
	fu := &fakeUsage{}
	srv := plainServer(fu, provider.NewMock(0, 0), false)

	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	ev := fu.one(t)
	if ev.RequestID != rec.Header().Get("X-Request-ID") || ev.RequestID == "" {
		t.Errorf("the event must carry the request id of the response: %q vs %q", ev.RequestID, rec.Header().Get("X-Request-ID"))
	}
	if ev.TenantID != testTenantID || ev.Provider != "mock" || ev.Status != "ok" || ev.CacheStatus != "OFF" {
		t.Errorf("wrong event: %+v", ev)
	}
	// The mock answers with 4 tokens and costs $10 per million tokens.
	if ev.CompletionTokens != 4 || math.Abs(ev.CostUSD-0.00004) > 1e-12 {
		t.Errorf("wrong tokens or cost: %+v", ev)
	}
	if ev.Model == "" {
		t.Error("the model must not be empty (the mock uses its own name)")
	}
	if time.Since(ev.CreatedAt) > time.Minute || ev.LatencyMS < 0 {
		t.Errorf("wrong time data: %+v", ev)
	}
}

func TestEventCostEqualsWhatTheBudgetWasCharged(t *testing.T) {
	fu := &fakeUsage{}
	fb := &fakeBudget{status: budgetOK()}
	h := &Handler{Providers: provider.Registry{"mock": provider.NewMock(0, 0)}, Tenants: newFakeStore(),
		AdminToken: testAdminToken, Usage: fu, Budget: fb}
	do(NewRouter(h, 1<<20), http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)

	if len(fb.added) != 1 || math.Abs(fu.one(t).CostUSD-fb.added[0]) > 1e-15 {
		t.Errorf("event cost %v must equal the budget charge %v", fu.all(), fb.added)
	}
}

func TestFailedRequestRecordsAnErrorEventWithNoCost(t *testing.T) {
	fu := &fakeUsage{}
	srv := plainServer(fu, provider.NewMock(0, 1), false) // the mock always fails

	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d", rec.Code)
	}
	ev := fu.one(t)
	if ev.Status != "error" || ev.CostUSD != 0 || ev.PromptTokens != 0 || ev.CompletionTokens != 0 {
		t.Errorf("a failure costs nothing: %+v", ev)
	}
}

func TestRejectedRequestsRecordNothing(t *testing.T) {
	fu := &fakeUsage{}
	// Rate limited:
	h := &Handler{Providers: provider.Registry{"mock": provider.NewMock(0, 0)}, Tenants: newFakeStore(), AdminToken: testAdminToken, Usage: fu,
		Limiter: &fakeLimiter{decision: ratelimit.Decision{Reason: "rpm", RetryAfter: time.Second}}}
	if rec := do(NewRouter(h, 1<<20), http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey); rec.Code != 429 {
		t.Fatalf("status %d", rec.Code)
	}
	// Budget used up:
	h.Limiter = nil
	h.Budget = &fakeBudget{status: usage.BudgetStatus{Allowed: false}}
	if rec := do(NewRouter(h, 1<<20), http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey); rec.Code != 402 {
		t.Fatalf("status %d", rec.Code)
	}
	// Bad request and bad key:
	srv := NewRouter(h, 1<<20)
	do(srv, http.MethodPost, "/v1/chat/completions", `{"model":""}`, "Bearer "+testAPIKey)
	do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer wrong")
	if n := len(fu.all()); n != 0 {
		t.Errorf("requests that never reached a provider or the cache must not be recorded, got %d events", n)
	}
}

func TestNoRecorderIsFine(t *testing.T) {
	h := &Handler{Providers: provider.Registry{"mock": provider.NewMock(0, 0)}, Tenants: newFakeStore(), AdminToken: testAdminToken}
	if rec := do(NewRouter(h, 1<<20), http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey); rec.Code != 200 {
		t.Errorf("status %d", rec.Code)
	}
}

// ---- with the cache ----

func TestCacheStatusesAreRecorded(t *testing.T) {
	fu := &fakeUsage{}
	srv := plainServer(fu, provider.NewMock(0, 0), true)
	send := func(body string) { do(srv, http.MethodPost, "/v1/chat/completions", body, "Bearer "+testAPIKey) }

	send(temp0Body) // miss: pays
	send(temp0Body) // exact hit: free
	send(temp7Body) // not cacheable: pays
	evs := fu.all()
	if len(evs) != 3 {
		t.Fatalf("3 requests, 3 events, got %d", len(evs))
	}
	if evs[0].CacheStatus != "MISS" || evs[0].CostUSD == 0 || evs[0].CompletionTokens == 0 {
		t.Errorf("miss: %+v", evs[0])
	}
	if evs[1].CacheStatus != "HIT-EXACT" || evs[1].CostUSD != 0 || evs[1].PromptTokens != 0 || evs[1].CompletionTokens != 0 || evs[1].Status != "ok" {
		t.Errorf("a hit costs nothing: %+v", evs[1])
	}
	if evs[1].Provider != "mock" {
		t.Errorf("a hit still names the provider that answered: %+v", evs[1])
	}
	if evs[2].CacheStatus != "BYPASS" || evs[2].CostUSD == 0 {
		t.Errorf("bypass: %+v", evs[2])
	}
	if evs[0].RequestID == evs[1].RequestID {
		t.Error("every request has its own id")
	}
}

func TestSemanticHitIsRecorded(t *testing.T) {
	rig, _, _ := newSemanticRig()
	fu := &fakeUsage{}
	h := &Handler{
		Providers: provider.Registry{"mock": rig.prov}, Tenants: rig.store, AdminToken: testAdminToken,
		Cache: rig.cache, Semantic: &memSemantic{threshold: 0.8}, Embedder: &countingEmbedder{inner: embed.NewMock(768)},
		Usage: fu,
	}
	srv := NewRouter(h, 1<<20)
	do(srv, http.MethodPost, "/v1/chat/completions", body("What is the capital of France"), "Bearer "+testAPIKey)
	do(srv, http.MethodPost, "/v1/chat/completions", body("What is the capital of France please"), "Bearer "+testAPIKey)

	evs := fu.all()
	if len(evs) != 2 || evs[1].CacheStatus != "HIT-SEMANTIC" || evs[1].CostUSD != 0 {
		t.Errorf("unexpected events: %+v", evs)
	}
}

// Ten identical requests: ten events, but only ONE of them has a cost.
func TestCoalescedRequestsAreRecordedAsFree(t *testing.T) {
	fu := &fakeUsage{}
	rig := newCacheRig(false)
	rig.prov.gate = make(chan struct{})
	h := &Handler{Providers: provider.Registry{"mock": rig.prov}, Tenants: rig.store, AdminToken: testAdminToken,
		Limiter: rig.lim, Budget: rig.bud, Cache: rig.cache, Usage: fu}
	rig.srv = NewRouter(h, 1<<20)

	done := make(chan struct{})
	go func() { fire(rig.srv, 10, temp0Body); close(done) }()
	waitStarted(t, rig.prov)
	time.Sleep(200 * time.Millisecond)
	close(rig.prov.gate)
	<-done

	evs := fu.all()
	if len(evs) != 10 {
		t.Fatalf("10 requests must give 10 events, got %d", len(evs))
	}
	counts := map[string]int{}
	ids := map[string]bool{}
	var total float64
	for _, e := range evs {
		counts[e.CacheStatus]++
		ids[e.RequestID] = true
		total += e.CostUSD
	}
	if counts["MISS"] != 1 || counts["COALESCED"] != 9 {
		t.Errorf("want 1 MISS and 9 COALESCED, got %v", counts)
	}
	if len(ids) != 10 {
		t.Errorf("every event needs its own request id, got %d different", len(ids))
	}
	if len(rig.bud.added) != 1 || math.Abs(total-rig.bud.added[0]) > 1e-15 {
		t.Errorf("total event cost %v must equal the single budget charge %v", total, rig.bud.added)
	}
}

func TestFailedCoalescedRequestsAreRecordedAsErrors(t *testing.T) {
	fu := &fakeUsage{}
	rig := newCacheRig(false)
	rig.prov.gate = make(chan struct{})
	rig.prov.err = &provider.ProviderError{StatusCode: 500, Message: "boom"}
	h := &Handler{Providers: provider.Registry{"mock": rig.prov}, Tenants: rig.store, AdminToken: testAdminToken,
		Cache: rig.cache, Usage: fu}
	rig.srv = NewRouter(h, 1<<20)

	done := make(chan struct{})
	go func() { fire(rig.srv, 4, temp0Body); close(done) }()
	waitStarted(t, rig.prov)
	time.Sleep(200 * time.Millisecond)
	close(rig.prov.gate)
	<-done

	evs := fu.all()
	if len(evs) != 4 {
		t.Fatalf("4 events expected, got %d", len(evs))
	}
	for _, e := range evs {
		if e.Status != "error" || e.CostUSD != 0 {
			t.Errorf("failed requests cost nothing: %+v", e)
		}
	}
}

// ---- streaming ----

func TestStreamRecordsAnEventWhenItEnds(t *testing.T) {
	fu := &fakeUsage{}
	srv := plainServer(fu, provider.NewMock(0, 0), false)
	rec := do(srv, http.MethodPost, "/v1/chat/completions",
		`{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "Bearer "+testAPIKey)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "[DONE]") {
		t.Fatalf("status %d", rec.Code)
	}
	ev := fu.one(t)
	if ev.Status != "ok" || ev.CompletionTokens == 0 || ev.CostUSD == 0 {
		t.Errorf("a finished stream is charged: %+v", ev)
	}
}

func TestStreamBrokenInTheMiddleIsRecorded(t *testing.T) {
	fu := &fakeUsage{}
	srv := plainServer(fu, provider.NewMock(0, 0), false)
	do(srv, http.MethodPost, "/v1/chat/completions",
		`{"model":"mock","stream":true,"messages":[{"role":"user","content":"please [fail-midstream]"}]}`, "Bearer "+testAPIKey)
	ev := fu.one(t)
	if ev.Status != "stream_error" {
		t.Errorf("status %q, want stream_error", ev.Status)
	}
	if ev.CostUSD == 0 {
		t.Error("the words that were already produced are charged")
	}
}

func TestStreamThatFailsToStartIsRecordedAsError(t *testing.T) {
	fu := &fakeUsage{}
	srv := plainServer(fu, provider.NewMock(0, 1), false)
	rec := do(srv, http.MethodPost, "/v1/chat/completions",
		`{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "Bearer "+testAPIKey)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d", rec.Code)
	}
	if ev := fu.one(t); ev.Status != "error" || ev.CostUSD != 0 {
		t.Errorf("%+v", ev)
	}
}

// When the client leaves in the middle of a stream we still record what was used.
func TestStreamCancelledByTheClientIsRecorded(t *testing.T) {
	fu := &fakeUsage{}
	h := &Handler{Providers: provider.Registry{"endless": &endlessProvider{stopped: make(chan struct{})}},
		Tenants: newFakeStore(), AdminToken: testAdminToken, Usage: fu}
	ts := httptest.NewServer(NewRouter(h, 1<<20))
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"endless/x","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(resp.Body)
	for i := 0; i < 5; i++ {
		reader.ReadString('\n')
	}
	cancel()
	resp.Body.Close()

	deadline := time.Now().Add(3 * time.Second)
	for len(fu.all()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	ev := fu.one(t)
	if ev.Status != "cancelled" {
		t.Errorf("status %q, want cancelled", ev.Status)
	}
}

// The event shows how long the request really took.
func TestLatencyIsMeasured(t *testing.T) {
	fu := &fakeUsage{}
	srv := plainServer(fu, provider.NewMock(150*time.Millisecond, 0), false)
	do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)

	ev := fu.one(t)
	if ev.LatencyMS < 150 || ev.LatencyMS > 5000 {
		t.Errorf("latency %d ms, the mock alone takes 150 ms", ev.LatencyMS)
	}
}

// brokenWriter stops accepting data after a few writes, like a client whose connection broke.
type brokenWriter struct {
	*httptest.ResponseRecorder
	writes int
}

func (b *brokenWriter) Write(p []byte) (int, error) {
	b.writes++
	if b.writes > 2 {
		return 0, context.Canceled
	}
	return b.ResponseRecorder.Write(p)
}

// When writing to the client fails, the stream stops and is recorded as cancelled.
func TestStreamWriteFailureIsRecordedAsCancelled(t *testing.T) {
	fu := &fakeUsage{}
	srv := plainServer(fu, provider.NewMock(0, 0), false)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"mock","stream":true,"messages":[{"role":"user","content":"one two three four five six"}]}`))
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	srv.ServeHTTP(&brokenWriter{ResponseRecorder: httptest.NewRecorder()}, req)

	if ev := fu.one(t); ev.Status != "cancelled" {
		t.Errorf("status %q, want cancelled", ev.Status)
	}
}
