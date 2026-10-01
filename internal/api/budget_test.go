package api

import (
	"context"
	"math"
	"net/http"
	"strings"
	"sync"
	"testing"

	"gatellm/internal/provider"
	"gatellm/internal/usage"
)

// fakeBudget answers with a fixed status and remembers what was added.
type fakeBudget struct {
	mu     sync.Mutex // the concurrency tests call it from many goroutines
	status usage.BudgetStatus
	err    error

	checkCalls  int
	lastBudget  float64
	lastTenant  string
	added       []float64
	addedTenant string
}

func (f *fakeBudget) Check(ctx context.Context, tenantID string, budgetUSD float64) (usage.BudgetStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checkCalls++
	f.lastTenant, f.lastBudget = tenantID, budgetUSD
	return f.status, f.err
}

func (f *fakeBudget) Add(ctx context.Context, tenantID string, usd float64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addedTenant = tenantID
	f.added = append(f.added, usd)
	return nil
}

func budgetOK() usage.BudgetStatus {
	return usage.BudgetStatus{Allowed: true, SpentUSD: 1, RemainingUSD: 4}
}

// serverWithBudget builds the router with a budget tracker (and no rate limiter).
func serverWithBudget(b BudgetTracker, rl RateLimiter, p provider.Provider) http.Handler {
	h := &Handler{
		Providers:  provider.Registry{"mock": p},
		Tenants:    newFakeStore(),
		AdminToken: testAdminToken,
		Budget:     b,
		Limiter:    rl,
	}
	return NewRouter(h, 1<<20)
}

func TestBudgetAllowsAndRecordsCost(t *testing.T) {
	fb := &fakeBudget{status: budgetOK()}
	srv := serverWithBudget(fb, nil, provider.NewMock(0, 0))

	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code != 200 {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body.String())
	}

	// The tenant's own budget (5 dollars in the fake store) is what we check against.
	if fb.checkCalls != 1 || fb.lastTenant != testTenantID || fb.lastBudget != 5 {
		t.Errorf("wrong Check call: %+v", fb)
	}
	if rec.Header().Get("X-Budget-Remaining-USD") != "4.000000" {
		t.Errorf("wrong remaining header: %q", rec.Header().Get("X-Budget-Remaining-USD"))
	}

	// The mock says prompt 0, completion 4, total 4 tokens. Mock price: $10 per 1M tokens.
	if len(fb.added) != 1 || fb.addedTenant != testTenantID {
		t.Fatalf("expected one Add call, got %+v", fb.added)
	}
	if math.Abs(fb.added[0]-0.00004) > 1e-12 {
		t.Errorf("cost = %v, want 0.00004", fb.added[0])
	}
}

func TestBudgetUsedUpGives402(t *testing.T) {
	fb := &fakeBudget{status: usage.BudgetStatus{Allowed: false, SpentUSD: 5}}
	counter := &countingProvider{}
	srv := serverWithBudget(fb, nil, counter)

	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code != http.StatusPaymentRequired {
		t.Fatalf("status %d, want 402", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "insufficient_quota") {
		t.Errorf("unexpected body: %s", rec.Body.String())
	}
	if rec.Header().Get("Retry-After") != "" {
		t.Error("waiting a few seconds does not help, so there must be no Retry-After")
	}
	if counter.calls != 0 {
		t.Error("the provider must not be called when the budget is used up")
	}
	if len(fb.added) != 0 {
		t.Error("nothing was used, so nothing must be added")
	}
}

func TestBudgetUsedUpAlsoBlocksStreaming(t *testing.T) {
	fb := &fakeBudget{status: usage.BudgetStatus{Allowed: false}}
	srv := serverWithBudget(fb, nil, provider.NewMock(0, 0))

	rec := do(srv, http.MethodPost, "/v1/chat/completions",
		`{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "Bearer "+testAPIKey)
	if rec.Code != http.StatusPaymentRequired {
		t.Errorf("status %d, want 402", rec.Code)
	}
}

func TestBudgetRedisDownFailClosedGives503(t *testing.T) {
	fb := &fakeBudget{err: usage.ErrUnavailable}
	srv := serverWithBudget(fb, nil, provider.NewMock(0, 0))

	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status %d, want 503", rec.Code)
	}
}

func TestBudgetRedisDownFailOpenLetsRequestPass(t *testing.T) {
	fb := &fakeBudget{status: usage.BudgetStatus{Allowed: true, Degraded: true}}
	srv := serverWithBudget(fb, nil, provider.NewMock(0, 0))

	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code != 200 {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	if rec.Header().Get("X-Budget-Remaining-USD") != "" {
		t.Error("we know nothing about the budget when Redis is down, so no header")
	}
}

// A request with a used-up budget is rejected BEFORE the rate limiter, so it costs no rate limit.
func TestBudgetIsCheckedBeforeRateLimit(t *testing.T) {
	fb := &fakeBudget{status: usage.BudgetStatus{Allowed: false}}
	fl := &fakeLimiter{decision: allowed()}
	srv := serverWithBudget(fb, fl, provider.NewMock(0, 0))

	do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if fl.allowCalls != 0 {
		t.Error("the rate limiter must not be used when the budget is already used up")
	}
}

func TestFailedProviderCallCostsNothing(t *testing.T) {
	fb := &fakeBudget{status: budgetOK()}
	srv := serverWithBudget(fb, nil, provider.NewMock(0, 1)) // always fails

	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", rec.Code)
	}
	if len(fb.added) != 0 {
		t.Errorf("a failed call must not be charged, got %v", fb.added)
	}
}

func TestStreamRecordsCost(t *testing.T) {
	fb := &fakeBudget{status: budgetOK()}
	srv := serverWithBudget(fb, nil, provider.NewMock(0, 0))

	rec := do(srv, http.MethodPost, "/v1/chat/completions",
		`{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "Bearer "+testAPIKey)
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	if len(fb.added) != 1 || fb.added[0] <= 0 {
		t.Errorf("a finished stream must be charged once, got %v", fb.added)
	}
}

func TestStreamFailingAtStartCostsNothing(t *testing.T) {
	fb := &fakeBudget{status: budgetOK()}
	srv := serverWithBudget(fb, nil, provider.NewMock(0, 1))

	do(srv, http.MethodPost, "/v1/chat/completions",
		`{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "Bearer "+testAPIKey)
	if len(fb.added) != 0 {
		t.Errorf("nothing was produced, so nothing is charged: %v", fb.added)
	}
}

func TestInvalidRequestDoesNotTouchBudget(t *testing.T) {
	fb := &fakeBudget{status: budgetOK()}
	srv := serverWithBudget(fb, nil, provider.NewMock(0, 0))

	do(srv, http.MethodPost, "/v1/chat/completions", `{"model":"mock","messages":[]}`, "Bearer "+testAPIKey)
	if fb.checkCalls != 0 {
		t.Error("an invalid request must not touch the budget")
	}
}

func TestCostTokens(t *testing.T) {
	p, c := costTokens(&provider.Usage{PromptTokens: 7, CompletionTokens: 9, TotalTokens: 16}, 100, 4000)
	if p != 7 || c != 9 {
		t.Errorf("real usage should win, got %d and %d", p, c)
	}
	// No usage: use the prompt estimate and answer chars / 4.
	p, c = costTokens(nil, 100, 400)
	if p != 100 || c != 100 {
		t.Errorf("fallback should be 100 and 100, got %d and %d", p, c)
	}
}

func TestNoBudgetTrackerMeansNoBudget(t *testing.T) {
	srv := serverWithBudget(nil, nil, provider.NewMock(0, 0))
	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code != 200 {
		t.Errorf("status %d, want 200", rec.Code)
	}
}
