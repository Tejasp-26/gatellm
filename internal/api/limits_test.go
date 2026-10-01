package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"gatellm/internal/provider"
	"gatellm/internal/ratelimit"
)

// fakeLimiter answers with a fixed decision and remembers what it was asked.
type fakeLimiter struct {
	mu       sync.Mutex // the concurrency tests call it from many goroutines
	decision ratelimit.Decision
	err      error

	allowCalls  int
	lastCost    int64
	lastLimits  ratelimit.Limits
	lastTenant  string
	adjustCalls int
	deltas      []int64
}

func (f *fakeLimiter) Allow(ctx context.Context, tenantID string, lim ratelimit.Limits, cost int64) (ratelimit.Decision, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allowCalls++
	f.lastCost, f.lastLimits, f.lastTenant = cost, lim, tenantID
	return f.decision, f.err
}

func (f *fakeLimiter) Adjust(ctx context.Context, tenantID string, lim ratelimit.Limits, delta int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.adjustCalls++
	f.deltas = append(f.deltas, delta)
	return nil
}

func allowed() ratelimit.Decision {
	return ratelimit.Decision{Allowed: true, RemainingRequests: 59, RemainingTokens: 19000}
}

// serverWithLimiter builds the router with a limiter and the given provider under the name "mock".
func serverWithLimiter(lim RateLimiter, p provider.Provider) http.Handler {
	h := &Handler{
		Providers:  provider.Registry{"mock": p},
		Tenants:    newFakeStore(),
		AdminToken: testAdminToken,
		Limiter:    lim,
	}
	return NewRouter(h, 1<<20)
}

const hiBody = `{"model":"mock","messages":[{"role":"user","content":"hi"}]}`

func TestEstimateTokens(t *testing.T) {
	// 40 characters = 10 tokens.
	req := &provider.ChatRequest{Messages: []provider.Message{{Role: "user", Content: strings.Repeat("a", 40)}}}
	if got := estimatePromptTokens(req); got != 10 {
		t.Errorf("prompt estimate = %d, want 10", got)
	}
	if got := estimateTokens(req); got != 10+defaultMaxTokens {
		t.Errorf("without max_tokens the estimate should add %d, got %d", defaultMaxTokens, got)
	}
	req.MaxTokens = 100
	if got := estimateTokens(req); got != 110 {
		t.Errorf("with max_tokens=100 the estimate should be 110, got %d", got)
	}
}

func TestActualTokens(t *testing.T) {
	if got := actualTokens(&provider.Usage{TotalTokens: 42}, 10, 400); got != 42 {
		t.Errorf("real usage should win, got %d", got)
	}
	// No usage from the provider: prompt estimate + answer chars / 4.
	if got := actualTokens(nil, 10, 400); got != 110 {
		t.Errorf("fallback should be 110, got %d", got)
	}
	if got := actualTokens(&provider.Usage{}, 10, 40); got != 20 {
		t.Errorf("zero usage should use the fallback, got %d", got)
	}
}

func TestLimiterAllowsAndSetsHeaders(t *testing.T) {
	fl := &fakeLimiter{decision: allowed()}
	srv := serverWithLimiter(fl, provider.NewMock(0, 0))

	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code != 200 {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body.String())
	}

	// The limiter was asked with the tenant's own limits and our estimate (2 chars/4=0, plus 256).
	if fl.allowCalls != 1 || fl.lastTenant != testTenantID {
		t.Errorf("wrong Allow call: %+v", fl)
	}
	if fl.lastLimits.RPM != 60 || fl.lastLimits.TPM != 20000 {
		t.Errorf("limits must come from the tenant: %+v", fl.lastLimits)
	}
	if fl.lastCost != defaultMaxTokens {
		t.Errorf("estimate = %d, want %d", fl.lastCost, defaultMaxTokens)
	}

	h := rec.Header()
	if h.Get("X-RateLimit-Limit-Requests") != "60" || h.Get("X-RateLimit-Remaining-Requests") != "59" ||
		h.Get("X-RateLimit-Limit-Tokens") != "20000" || h.Get("X-RateLimit-Remaining-Tokens") != "19000" {
		t.Errorf("wrong rate limit headers: %v", h)
	}

	// The estimate (256) is replaced by the real usage from the mock.
	if fl.adjustCalls != 1 {
		t.Fatalf("expected one Adjust call, got %d", fl.adjustCalls)
	}
	if fl.deltas[0] >= 0 {
		t.Errorf("real usage is smaller than the estimate, so delta must be negative, got %d", fl.deltas[0])
	}
}

func TestLimiterDeniesWith429(t *testing.T) {
	tests := []struct {
		reason string
		wants  string
	}{
		{"rpm", "requests per minute"},
		{"tpm", "tokens per minute"},
	}
	for _, tc := range tests {
		t.Run(tc.reason, func(t *testing.T) {
			fl := &fakeLimiter{decision: ratelimit.Decision{Reason: tc.reason, RetryAfter: 2300 * time.Millisecond}}
			srv := serverWithLimiter(fl, provider.NewMock(0, 0))

			rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
			if rec.Code != http.StatusTooManyRequests {
				t.Fatalf("status %d, want 429", rec.Code)
			}
			if rec.Header().Get("Retry-After") != "3" { // 2.3 seconds rounds UP
				t.Errorf("Retry-After = %q, want 3", rec.Header().Get("Retry-After"))
			}
			body := rec.Body.String()
			if !strings.Contains(body, "rate_limit_error") || !strings.Contains(body, tc.wants) {
				t.Errorf("unexpected body: %s", body)
			}
			if fl.adjustCalls != 0 {
				t.Error("a denied request used nothing, so nothing must be adjusted")
			}
		})
	}
}

func TestDeniedRequestNeverReachesProvider(t *testing.T) {
	counter := &countingProvider{}
	fl := &fakeLimiter{decision: ratelimit.Decision{Reason: "rpm", RetryAfter: time.Second}}
	srv := serverWithLimiter(fl, counter)

	do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if counter.calls != 0 {
		t.Error("the provider must not be called when the rate limit says no")
	}
}

type countingProvider struct{ calls int }

func (c *countingProvider) Name() string { return "mock" }
func (c *countingProvider) Chat(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	c.calls++
	return nil, errors.New("should not be called")
}
func (c *countingProvider) ChatStream(ctx context.Context, req *provider.ChatRequest) (<-chan provider.StreamChunk, error) {
	c.calls++
	return nil, errors.New("should not be called")
}

func TestRedisDownFailClosedGives503(t *testing.T) {
	fl := &fakeLimiter{err: ratelimit.ErrUnavailable}
	srv := serverWithLimiter(fl, provider.NewMock(0, 0))

	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status %d, want 503", rec.Code)
	}
}

func TestRedisDownFailOpenLetsRequestPass(t *testing.T) {
	fl := &fakeLimiter{decision: ratelimit.Decision{Allowed: true, Degraded: true}}
	srv := serverWithLimiter(fl, provider.NewMock(0, 0))

	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code != 200 {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	if rec.Header().Get("X-RateLimit-Limit-Requests") != "" {
		t.Error("we know nothing about the limits when Redis is down, so no headers")
	}
	if fl.adjustCalls != 0 {
		t.Error("nothing was reserved, so nothing can be adjusted")
	}
}

func TestRequestBiggerThanTheTokenLimitGets400(t *testing.T) {
	fl := &fakeLimiter{decision: allowed()}
	srv := serverWithLimiter(fl, provider.NewMock(0, 0))

	// The test tenant has 20000 tokens per minute. This asks for 30000 tokens of answer.
	body := `{"model":"mock","max_tokens":30000,"messages":[{"role":"user","content":"hi"}]}`
	rec := do(srv, http.MethodPost, "/v1/chat/completions", body, "Bearer "+testAPIKey)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status %d, want 400", rec.Code)
	}
	if fl.allowCalls != 0 {
		t.Error("an impossible request should be rejected without asking Redis")
	}
}

func TestProviderFailureRefundsTheEstimate(t *testing.T) {
	fl := &fakeLimiter{decision: allowed()}
	srv := serverWithLimiter(fl, provider.NewMock(0, 1)) // the mock always fails

	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", rec.Code)
	}
	if len(fl.deltas) != 1 || fl.deltas[0] != -fl.lastCost {
		t.Errorf("the whole estimate (%d) should be given back, got %v", fl.lastCost, fl.deltas)
	}
}

func TestStreamSettlesWithRealUsage(t *testing.T) {
	fl := &fakeLimiter{decision: allowed()}
	srv := serverWithLimiter(fl, provider.NewMock(0, 0))

	rec := do(srv, http.MethodPost, "/v1/chat/completions",
		`{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "Bearer "+testAPIKey)
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	// "Mock reply to: hi" is 17 characters; the mock reports about 17/4 + prompt tokens, far below 256.
	if len(fl.deltas) != 1 || fl.deltas[0] >= 0 {
		t.Errorf("after the stream the estimate must be corrected downwards, got %v", fl.deltas)
	}
}

func TestStreamFailingAtStartRefunds(t *testing.T) {
	fl := &fakeLimiter{decision: allowed()}
	srv := serverWithLimiter(fl, provider.NewMock(0, 1))

	rec := do(srv, http.MethodPost, "/v1/chat/completions",
		`{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "Bearer "+testAPIKey)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", rec.Code)
	}
	if len(fl.deltas) != 1 || fl.deltas[0] != -fl.lastCost {
		t.Errorf("the whole estimate should be given back, got %v (cost %d)", fl.deltas, fl.lastCost)
	}
}

// Bad requests are rejected before the limiter, so they do not use the quota.
func TestInvalidRequestDoesNotUseQuota(t *testing.T) {
	fl := &fakeLimiter{decision: allowed()}
	srv := serverWithLimiter(fl, provider.NewMock(0, 0))

	do(srv, http.MethodPost, "/v1/chat/completions", `{"model":"mock","messages":[]}`, "Bearer "+testAPIKey)
	if fl.allowCalls != 0 {
		t.Error("an invalid request must not touch the rate limiter")
	}
}

func TestNoLimiterMeansNoLimiting(t *testing.T) {
	srv := serverWithLimiter(nil, provider.NewMock(0, 0))
	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code != 200 {
		t.Errorf("status %d, want 200", rec.Code)
	}
}
