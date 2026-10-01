package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gatellm/internal/cache"
	"gatellm/internal/provider"
	"gatellm/internal/ratelimit"
	"gatellm/internal/store"
	"gatellm/internal/usage"
)

// ---- fakes ----

// memCache is a cache in memory. It can also be told to fail.
type memCache struct {
	mu     sync.Mutex
	data   map[string]*cache.Entry
	gets   int
	sets   int
	getErr error
	setErr error
}

func newMemCache() *memCache { return &memCache{data: map[string]*cache.Entry{}} }

func (c *memCache) Get(ctx context.Context, key string) (*cache.Entry, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets++
	if c.getErr != nil {
		return nil, false, c.getErr
	}
	e, ok := c.data[key]
	return e, ok, nil
}

func (c *memCache) Set(ctx context.Context, key string, e *cache.Entry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sets++
	if c.setErr != nil {
		return c.setErr
	}
	c.data[key] = e
	return nil
}

func (c *memCache) counts() (gets, sets, size int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets, c.sets, len(c.data)
}

// lateCache misses on the first Get and "finds" the answer on later ones.
// It acts like another request that filled the cache just after our first look.
type lateCache struct {
	mu    sync.Mutex
	gets  int
	entry *cache.Entry
}

func (c *lateCache) Get(ctx context.Context, key string) (*cache.Entry, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets++
	if c.gets == 1 {
		return nil, false, nil
	}
	return c.entry, true, nil
}
func (c *lateCache) Set(ctx context.Context, key string, e *cache.Entry) error { return nil }

// gateProvider counts calls. If gate is set, every call waits until it is closed.
// It stops when its own context is cancelled (so we can prove the context is not cancelled).
type gateProvider struct {
	calls   atomic.Int64
	started chan struct{} // gets a value when a call has started
	gate    chan struct{}
	err     error
	empty   bool // answer with no choices at all
}

func newGateProvider() *gateProvider { return &gateProvider{started: make(chan struct{}, 100)} }

func (p *gateProvider) Name() string { return "mock" }
func (p *gateProvider) Chat(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	p.calls.Add(1)
	p.started <- struct{}{}
	if p.gate != nil {
		select {
		case <-p.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if p.err != nil {
		return nil, p.err
	}
	if p.empty {
		return &provider.ChatResponse{ID: "chatcmpl-empty"}, nil
	}
	return &provider.ChatResponse{
		ID: "chatcmpl-1", Object: "chat.completion", Model: "mock",
		Choices: []provider.Choice{{Message: provider.Message{Role: "assistant", Content: "the answer"}, FinishReason: "stop"}},
		Usage:   provider.Usage{PromptTokens: 5, CompletionTokens: 7, TotalTokens: 12},
	}, nil
}
func (p *gateProvider) ChatStream(ctx context.Context, req *provider.ChatRequest) (<-chan provider.StreamChunk, error) {
	return provider.NewMock(0, 0).ChatStream(ctx, req)
}

// ---- helpers ----

type cacheRig struct {
	srv   http.Handler
	cache *memCache
	prov  *gateProvider
	lim   *fakeLimiter
	bud   *fakeBudget
	store *fakeStore
}

func newCacheRig(allowTemp bool) *cacheRig {
	rig := &cacheRig{
		cache: newMemCache(), prov: newGateProvider(),
		lim: &fakeLimiter{decision: allowed()},
		bud: &fakeBudget{status: budgetOK()},
	}
	rig.store = newFakeStore()
	h := &Handler{
		Providers:  provider.Registry{"mock": rig.prov},
		Tenants:    rig.store,
		AdminToken: testAdminToken,
		Limiter:    rig.lim,
		Budget:     rig.bud,
		Cache:      rig.cache,

		CacheAllowTemperature: allowTemp,
	}
	rig.srv = NewRouter(h, 1<<20)
	return rig
}

func (r *cacheRig) post(body string) *httptest.ResponseRecorder {
	return do(r.srv, http.MethodPost, "/v1/chat/completions", body, "Bearer "+testAPIKey)
}

const (
	temp0Body = `{"model":"mock","temperature":0,"messages":[{"role":"user","content":"What is Go?"}]}`
	temp7Body = `{"model":"mock","temperature":0.7,"messages":[{"role":"user","content":"What is Go?"}]}`
	noTemp    = `{"model":"mock","messages":[{"role":"user","content":"What is Go?"}]}`
	streamTmp = `{"model":"mock","temperature":0,"stream":true,"messages":[{"role":"user","content":"What is Go?"}]}`
)

// ---- tests ----

func TestMissThenHit(t *testing.T) {
	rig := newCacheRig(false)

	first := rig.post(temp0Body)
	if first.Code != 200 || first.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("first: status %d, X-Cache %q, body %s", first.Code, first.Header().Get("X-Cache"), first.Body.String())
	}
	second := rig.post(temp0Body)
	if second.Code != 200 || second.Header().Get("X-Cache") != "HIT-EXACT" {
		t.Fatalf("second: status %d, X-Cache %q", second.Code, second.Header().Get("X-Cache"))
	}
	if got := rig.prov.calls.Load(); got != 1 {
		t.Errorf("the provider must be called once, was called %d times", got)
	}
	if first.Body.String() != second.Body.String() {
		t.Error("a cache hit must return exactly the same answer")
	}
	if second.Header().Get("X-Provider") != "mock" {
		t.Errorf("a hit still names the provider that really answered, got %q", second.Header().Get("X-Provider"))
	}
	if second.Header().Get("X-Coalesced") != "" {
		t.Error("a plain hit is not coalesced")
	}
}

func TestRequestsThatMustNotBeCachedAreBypassed(t *testing.T) {
	for name, body := range map[string]string{"temperature 0.7": temp7Body, "no temperature": noTemp, "stream": streamTmp} {
		t.Run(name, func(t *testing.T) {
			rig := newCacheRig(false)
			a, b := rig.post(body), rig.post(body)
			if a.Code != 200 || b.Code != 200 {
				t.Fatalf("status %d / %d", a.Code, b.Code)
			}
			if a.Header().Get("X-Cache") != "BYPASS" || b.Header().Get("X-Cache") != "BYPASS" {
				t.Errorf("want BYPASS, got %q / %q", a.Header().Get("X-Cache"), b.Header().Get("X-Cache"))
			}
			if _, sets, size := rig.cache.counts(); sets != 0 || size != 0 {
				t.Error("nothing may be stored for these requests")
			}
			if gets, _, _ := rig.cache.counts(); gets != 0 {
				t.Error("the cache must not even be looked at")
			}
		})
	}
}

func TestAllowTemperatureFlagCachesTheRest(t *testing.T) {
	rig := newCacheRig(true)
	rig.post(temp7Body)
	again := rig.post(temp7Body)
	if again.Header().Get("X-Cache") != "HIT-EXACT" {
		t.Errorf("with the flag on, temperature 0.7 is cached, got %q", again.Header().Get("X-Cache"))
	}
	rig.post(noTemp)
	if rig.post(noTemp).Header().Get("X-Cache") != "HIT-EXACT" {
		t.Error("with the flag on, no temperature is cached too")
	}
	// Streaming is never cached, flag or not.
	if rig.post(streamTmp).Header().Get("X-Cache") != "BYPASS" {
		t.Error("streams are never cached")
	}
}

func TestDifferentRequestsDoNotShareAnAnswer(t *testing.T) {
	rig := newCacheRig(false)
	rig.post(temp0Body)
	other := rig.post(`{"model":"mock","temperature":0,"messages":[{"role":"user","content":"What is Rust?"}]}`)
	if other.Header().Get("X-Cache") != "MISS" {
		t.Errorf("a different question must be a miss, got %q", other.Header().Get("X-Cache"))
	}
	if rig.prov.calls.Load() != 2 {
		t.Error("two different questions need two provider calls")
	}
}

func TestNoCacheMeansNoHeader(t *testing.T) {
	srv := serverWithLimiter(nil, provider.NewMock(0, 0)) // Cache is nil
	rec := do(srv, http.MethodPost, "/v1/chat/completions", temp0Body, "Bearer "+testAPIKey)
	if rec.Code != 200 || rec.Header().Get("X-Cache") != "" {
		t.Errorf("with the cache off there must be no X-Cache header, got %q", rec.Header().Get("X-Cache"))
	}
}

// A hit costs nothing: the reserved tokens come back, and no money is added.
func TestHitRefundsTokensAndChargesNothing(t *testing.T) {
	rig := newCacheRig(false)
	rig.post(temp0Body) // miss: pays
	if len(rig.bud.added) != 1 {
		t.Fatalf("the miss must be charged once, got %v", rig.bud.added)
	}
	allowBefore, deltasBefore := rig.lim.allowCalls, len(rig.lim.deltas)

	rig.post(temp0Body) // hit

	if len(rig.bud.added) != 1 {
		t.Errorf("a hit must not add cost, added: %v", rig.bud.added)
	}
	if rig.lim.allowCalls != allowBefore+1 {
		t.Error("a hit still counts as one request for the rpm limit")
	}
	if len(rig.lim.deltas) != deltasBefore+1 || rig.lim.deltas[len(rig.lim.deltas)-1] != -rig.lim.lastCost {
		t.Errorf("a hit must give back all reserved tokens (%d), deltas: %v", rig.lim.lastCost, rig.lim.deltas)
	}
}

func TestMissSettlesRealTokensOnce(t *testing.T) {
	rig := newCacheRig(false)
	rig.post(temp0Body)
	// reserved lastCost, then corrected to the real 12 tokens.
	if net := rig.lim.lastCost + sum(rig.lim.deltas); net != 12 {
		t.Errorf("net tokens used = %d, want 12", net)
	}
}

func TestBudgetUsedUpBlocksEvenCachedAnswers(t *testing.T) {
	rig := newCacheRig(false)
	rig.post(temp0Body) // fills the cache
	rig.bud.mu.Lock()
	rig.bud.status = usage.BudgetStatus{Allowed: false}
	rig.bud.mu.Unlock()

	rec := rig.post(temp0Body)
	if rec.Code != http.StatusPaymentRequired {
		t.Errorf("status %d, want 402: the budget check comes before the cache", rec.Code)
	}
}

func TestRateLimitedRequestNeverTouchesTheCache(t *testing.T) {
	rig := newCacheRig(false)
	rig.lim.decision = ratelimit.Decision{Reason: "rpm", RetryAfter: time.Second}
	rec := rig.post(temp0Body)
	if rec.Code != 429 {
		t.Fatalf("status %d, want 429", rec.Code)
	}
	if gets, _, _ := rig.cache.counts(); gets != 0 {
		t.Error("a rejected request must not read the cache")
	}
}

func TestFailedAnswersAreNotCached(t *testing.T) {
	rig := newCacheRig(false)
	rig.prov.err = &provider.ProviderError{StatusCode: 500, Message: "boom"}

	rec := rig.post(temp0Body)
	if rec.Code != http.StatusBadGateway || rec.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("status %d, X-Cache %q", rec.Code, rec.Header().Get("X-Cache"))
	}
	if _, sets, size := rig.cache.counts(); sets != 0 || size != 0 {
		t.Error("an error must never be stored")
	}
	if net := rig.lim.lastCost + sum(rig.lim.deltas); net != 0 {
		t.Errorf("a failed call must give all tokens back, net %d", net)
	}

	rig.prov.err = nil // the provider recovers
	ok := rig.post(temp0Body)
	if ok.Code != 200 || ok.Header().Get("X-Cache") != "MISS" {
		t.Errorf("after recovery: status %d, X-Cache %q", ok.Code, ok.Header().Get("X-Cache"))
	}
	if rig.prov.calls.Load() != 2 {
		t.Error("the second request had to call the provider again")
	}
}

func TestCacheErrorsNeverBreakTheRequest(t *testing.T) {
	rig := newCacheRig(false)
	rig.cache.getErr = errors.New("redis is down")
	rig.cache.setErr = errors.New("redis is down")

	rec := rig.post(temp0Body)
	if rec.Code != 200 || rec.Header().Get("X-Cache") != "MISS" {
		t.Errorf("a broken cache must act like a miss: status %d, X-Cache %q", rec.Code, rec.Header().Get("X-Cache"))
	}
	if !strings.Contains(rec.Body.String(), "the answer") {
		t.Errorf("the client must still get the real answer: %s", rec.Body.String())
	}
}

// Another request filled the cache between our first look and becoming the leader.
func TestLeaderDoubleChecksTheCache(t *testing.T) {
	prov := newGateProvider()
	lim := &fakeLimiter{decision: allowed()}
	bud := &fakeBudget{status: budgetOK()}
	late := &lateCache{entry: &cache.Entry{Provider: "mock", Model: "", Response: &provider.ChatResponse{
		Choices: []provider.Choice{{Message: provider.Message{Role: "assistant", Content: "from the cache"}}},
	}}}
	h := &Handler{
		Providers: provider.Registry{"mock": prov}, Tenants: newFakeStore(), AdminToken: testAdminToken,
		Limiter: lim, Budget: bud, Cache: late,
	}
	rec := do(NewRouter(h, 1<<20), http.MethodPost, "/v1/chat/completions", temp0Body, "Bearer "+testAPIKey)

	if rec.Code != 200 || rec.Header().Get("X-Cache") != "HIT-EXACT" || !strings.Contains(rec.Body.String(), "from the cache") {
		t.Fatalf("status %d, X-Cache %q, body %s", rec.Code, rec.Header().Get("X-Cache"), rec.Body.String())
	}
	if prov.calls.Load() != 0 {
		t.Error("the provider must not be called when the double check finds the answer")
	}
	if len(bud.added) != 0 {
		t.Error("nothing was used, nothing may be charged")
	}
	if net := lim.lastCost + sum(lim.deltas); net != 0 {
		t.Errorf("the reserved tokens must come back, net %d", net)
	}
}

// ---- many requests at the same time ----

// fire sends n identical requests at once and returns the responses.
func fire(srv http.Handler, n int, body string) []*httptest.ResponseRecorder {
	out := make([]*httptest.ResponseRecorder, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out[i] = do(srv, http.MethodPost, "/v1/chat/completions", body, "Bearer "+testAPIKey)
		}(i)
	}
	wg.Wait()
	return out
}

// waitStarted waits until the provider got its first call.
func waitStarted(t *testing.T, p *gateProvider) {
	t.Helper()
	select {
	case <-p.started:
	case <-time.After(3 * time.Second):
		t.Fatal("the provider was never called")
	}
}

func TestTenIdenticalRequestsCallTheProviderOnce(t *testing.T) {
	rig := newCacheRig(false)
	rig.prov.gate = make(chan struct{})

	const n = 10
	done := make(chan []*httptest.ResponseRecorder)
	go func() { done <- fire(rig.srv, n, temp0Body) }()

	waitStarted(t, rig.prov)
	time.Sleep(200 * time.Millisecond) // let the other 9 arrive and wait
	close(rig.prov.gate)
	recs := <-done

	if got := rig.prov.calls.Load(); got != 1 {
		t.Fatalf("the provider must be called ONCE for %d identical requests, got %d", n, got)
	}
	leaders, followers := 0, 0
	for i, rec := range recs {
		if rec.Code != 200 || rec.Header().Get("X-Cache") != "MISS" {
			t.Fatalf("request %d: status %d, X-Cache %q", i, rec.Code, rec.Header().Get("X-Cache"))
		}
		if rec.Header().Get("X-Coalesced") == "true" {
			followers++
		} else {
			leaders++
		}
		if rec.Body.String() != recs[0].Body.String() {
			t.Errorf("request %d got a different answer", i)
		}
	}
	if leaders != 1 || followers != n-1 {
		t.Errorf("want 1 leader and %d followers, got %d and %d", n-1, leaders, followers)
	}

	// Money and tokens are charged exactly once, although 10 requests were answered.
	if len(rig.bud.added) != 1 {
		t.Errorf("the cost must be added once, added: %v", rig.bud.added)
	}
	rig.lim.mu.Lock()
	net := int64(rig.lim.allowCalls)*rig.lim.lastCost + sum(rig.lim.deltas)
	rig.lim.mu.Unlock()
	if net != 12 {
		t.Errorf("net tokens used = %d, want 12 (one real call)", net)
	}
	if _, sets, _ := rig.cache.counts(); sets != 1 {
		t.Errorf("the answer is stored once, was stored %d times", sets)
	}
}

func TestFollowersShareTheErrorAndGetTheirTokensBack(t *testing.T) {
	rig := newCacheRig(false)
	rig.prov.gate = make(chan struct{})
	rig.prov.err = &provider.ProviderError{StatusCode: 500, Message: "boom"}

	const n = 5
	done := make(chan []*httptest.ResponseRecorder)
	go func() { done <- fire(rig.srv, n, temp0Body) }()
	waitStarted(t, rig.prov)
	time.Sleep(200 * time.Millisecond)
	close(rig.prov.gate)

	for i, rec := range <-done {
		if rec.Code != http.StatusBadGateway {
			t.Errorf("request %d: status %d, want 502", i, rec.Code)
		}
	}
	if got := rig.prov.calls.Load(); got != 1 {
		t.Errorf("one failing call for everybody, got %d", got)
	}
	rig.lim.mu.Lock()
	net := int64(rig.lim.allowCalls)*rig.lim.lastCost + sum(rig.lim.deltas)
	rig.lim.mu.Unlock()
	if net != 0 {
		t.Errorf("nothing was used, net tokens must be 0, got %d", net)
	}
	if len(rig.bud.added) != 0 {
		t.Error("nothing may be charged for a failure")
	}
}

// The first client (the leader) gives up while the provider is still working.
// The others are waiting for the same call and must still get their answer.
func TestLeaderLeavingDoesNotKillTheFollowers(t *testing.T) {
	rig := newCacheRig(false)
	rig.prov.gate = make(chan struct{})

	leaderCtx, leaderLeaves := context.WithCancel(context.Background())
	leaderReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(temp0Body)).WithContext(leaderCtx)
	leaderReq.Header.Set("Authorization", "Bearer "+testAPIKey)
	leaderRec := httptest.NewRecorder()
	leaderDone := make(chan struct{})
	go func() { rig.srv.ServeHTTP(leaderRec, leaderReq); close(leaderDone) }()
	waitStarted(t, rig.prov) // the leader is now talking to the provider

	followerDone := make(chan *httptest.ResponseRecorder)
	go func() {
		followerDone <- do(rig.srv, http.MethodPost, "/v1/chat/completions", temp0Body, "Bearer "+testAPIKey)
	}()
	time.Sleep(150 * time.Millisecond) // the follower is waiting

	leaderLeaves()                     // the leader's client disconnects
	time.Sleep(100 * time.Millisecond) // give a wrong implementation time to cancel the call
	close(rig.prov.gate)               // the provider answers

	rec := <-followerDone
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "the answer") {
		t.Fatalf("the follower must get its answer, got %d %s", rec.Code, rec.Body.String())
	}
	<-leaderDone
	if got := rig.prov.calls.Load(); got != 1 {
		t.Errorf("provider calls = %d, want 1", got)
	}
	if _, sets, _ := rig.cache.counts(); sets != 1 {
		t.Error("the answer must still be stored although the leader left")
	}
}

func TestTenantsNeverShareCachedAnswers(t *testing.T) {
	rig := newCacheRig(false)
	// A second tenant with its own key.
	rig.store.tenants["tenant-b"] = &store.Tenant{ID: "tenant-b", Name: "b", RPMLimit: 60, TPMLimit: 20000, MonthlyBudgetUSD: 5}
	rig.store.keys[hashKey("gk_key_b")] = "tenant-b"

	rig.post(temp0Body) // tenant A fills the cache
	b := do(rig.srv, http.MethodPost, "/v1/chat/completions", temp0Body, "Bearer gk_key_b")
	if b.Code != 200 || b.Header().Get("X-Cache") != "MISS" {
		t.Errorf("tenant B must not get tenant A's cached answer: status %d, X-Cache %q", b.Code, b.Header().Get("X-Cache"))
	}
	if rig.prov.calls.Load() != 2 {
		t.Error("each tenant needs its own provider call")
	}
}

func sum(xs []int64) int64 {
	var s int64
	for _, x := range xs {
		s += x
	}
	return s
}

// A strange answer without any choice is passed on, but never stored.
func TestAnswerWithoutChoicesIsNotCached(t *testing.T) {
	rig := newCacheRig(false)
	rig.prov.empty = true
	rec := rig.post(temp0Body)
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	if _, sets, size := rig.cache.counts(); sets != 0 || size != 0 {
		t.Error("an answer without choices must not be stored")
	}
}
