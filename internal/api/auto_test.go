package api

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"gatellm/internal/provider"
	"gatellm/internal/router"
	"gatellm/internal/usage"
)

// autoServer builds the real router with the given providers (in priority order).
func autoServer(t *testing.T, bud BudgetTracker, providers ...provider.Provider) http.Handler {
	t.Helper()
	reg := provider.Registry{}
	var names []string
	for _, p := range providers {
		reg[p.Name()] = p
		names = append(names, p.Name())
	}
	targets, err := router.ParseTargets(strings.Join(names, ","), reg)
	if err != nil {
		t.Fatal(err)
	}
	strategy, _ := router.NewStrategy("priority")

	h := &Handler{
		Providers:  reg,
		Tenants:    newFakeStore(),
		AdminToken: testAdminToken,
		Router:     router.New(targets, strategy),
		Budget:     bud,
	}
	return NewRouter(h, 1<<20)
}

const autoBody = `{"model":"auto","messages":[{"role":"user","content":"hi"}]}`

func TestAutoUsesTheFirstProvider(t *testing.T) {
	srv := autoServer(t, nil, provider.NewMock(0, 0), provider.NewMockNamed("mock-b", 0, 0))
	rec := do(srv, http.MethodPost, "/v1/chat/completions", autoBody, "Bearer "+testAPIKey)

	if rec.Code != 200 {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Provider") != "mock" || rec.Header().Get("X-Route-Tried") != "mock" {
		t.Errorf("headers: provider %q tried %q", rec.Header().Get("X-Provider"), rec.Header().Get("X-Route-Tried"))
	}
}

func TestAutoFallsBackWhenTheFirstProviderFails(t *testing.T) {
	srv := autoServer(t, nil, provider.NewMock(0, 1), provider.NewMockNamed("mock-b", 0, 0)) // mock always fails
	rec := do(srv, http.MethodPost, "/v1/chat/completions", autoBody, "Bearer "+testAPIKey)

	if rec.Code != 200 {
		t.Fatalf("fallback should save the request, status %d, body %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Provider") != "mock-b" {
		t.Errorf("X-Provider = %q, want mock-b", rec.Header().Get("X-Provider"))
	}
	if rec.Header().Get("X-Route-Tried") != "mock,mock-b" {
		t.Errorf("X-Route-Tried = %q, want mock,mock-b", rec.Header().Get("X-Route-Tried"))
	}
	if !strings.Contains(rec.Body.String(), "Mock reply to: hi") {
		t.Errorf("unexpected body: %s", rec.Body.String())
	}
}

func TestAutoStreamFallsBack(t *testing.T) {
	srv := autoServer(t, nil, provider.NewMock(0, 1), provider.NewMockNamed("mock-b", 0, 0))
	rec := do(srv, http.MethodPost, "/v1/chat/completions",
		`{"model":"auto","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "Bearer "+testAPIKey)

	if rec.Code != 200 || rec.Header().Get("X-Provider") != "mock-b" {
		t.Fatalf("status %d, provider %q", rec.Code, rec.Header().Get("X-Provider"))
	}
	if rec.Header().Get("X-Route-Tried") != "mock,mock-b" {
		t.Errorf("X-Route-Tried = %q", rec.Header().Get("X-Route-Tried"))
	}
	ev := events(rec.Body.String())
	if ev[len(ev)-1] != "[DONE]" {
		t.Errorf("the stream should finish normally: %v", ev)
	}
}

func TestAutoAllProvidersFail(t *testing.T) {
	srv := autoServer(t, nil, provider.NewMock(0, 1), provider.NewMockNamed("mock-b", 0, 1))
	rec := do(srv, http.MethodPost, "/v1/chat/completions", autoBody, "Bearer "+testAPIKey)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "all providers failed (tried: mock, mock-b)") {
		t.Errorf("unexpected body: %s", rec.Body.String())
	}
	if rec.Header().Get("X-Route-Tried") != "mock,mock-b" {
		t.Errorf("X-Route-Tried = %q", rec.Header().Get("X-Route-Tried"))
	}
}

func TestAutoWithOpenCircuitsGives503(t *testing.T) {
	h := &Handler{ // one provider whose circuit breaker is open

		Providers: provider.Registry{"mock": openProvider{}}, Tenants: newFakeStore(), AdminToken: testAdminToken,
	}
	targets, _ := router.ParseTargets("mock", h.Providers)
	strategy, _ := router.NewStrategy("priority")
	h.Router = router.New(targets, strategy)
	srv := NewRouter(h, 1<<20)

	rec := do(srv, http.MethodPost, "/v1/chat/completions", autoBody, "Bearer "+testAPIKey)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status %d, want 503", rec.Code)
	}
}

func TestAutoDoesNotFallBackOnABadRequest(t *testing.T) {
	bad := &statusProvider{name: "mock", code: 400}
	good := provider.NewMockNamed("mock-b", 0, 0)
	srv := autoServer(t, nil, bad, good)
	rec := do(srv, http.MethodPost, "/v1/chat/completions", autoBody, "Bearer "+testAPIKey)

	if rec.Code != http.StatusBadGateway {
		t.Errorf("status %d, want 502 (the request is not sent to other providers)", rec.Code)
	}
	if rec.Header().Get("X-Route-Tried") != "mock" {
		t.Errorf("X-Route-Tried = %q, want only mock", rec.Header().Get("X-Route-Tried"))
	}
}

// statusProvider always fails with the given HTTP status.
type statusProvider struct {
	name string
	code int
}

func (s *statusProvider) Name() string { return s.name }
func (s *statusProvider) Chat(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	return nil, &provider.ProviderError{StatusCode: s.code}
}
func (s *statusProvider) ChatStream(ctx context.Context, req *provider.ChatRequest) (<-chan provider.StreamChunk, error) {
	return nil, &provider.ProviderError{StatusCode: s.code}
}

// The cost must be calculated for the provider that really answered, not the first one.
func TestAutoChargesTheProviderThatAnswered(t *testing.T) {
	fb := &fakeBudget{status: budgetOK()}
	srv := autoServer(t, fb, provider.NewMock(0, 1), provider.NewMockNamed("mock-b", 0, 0))
	do(srv, http.MethodPost, "/v1/chat/completions", autoBody, "Bearer "+testAPIKey)

	if len(fb.added) != 1 || fb.added[0] <= 0 {
		t.Fatalf("the answered request must be charged once: %v", fb.added)
	}
	// "mock-b" must have the (fake) mock price, not the default price of an unknown provider.
	want := usage.CostUSD("mock", "", 0, 4)
	if fb.added[0] != want {
		t.Errorf("cost = %v, want %v", fb.added[0], want)
	}
}

func TestAutoNotAvailableWithoutARouter(t *testing.T) {
	srv, _ := newTestServer(testAdminToken) // this handler has no router
	rec := do(srv, http.MethodPost, "/v1/chat/completions", autoBody, "Bearer "+testAPIKey)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "not available") {
		t.Errorf("status %d, body %s", rec.Code, rec.Body.String())
	}
}

func TestPinnedModelStillWorksAndHasNoFallback(t *testing.T) {
	reg := provider.Registry{"mock": provider.NewMock(0, 1), "mock-b": provider.NewMockNamed("mock-b", 0, 0)}
	h := &Handler{Providers: reg, Tenants: newFakeStore(), AdminToken: testAdminToken}
	srv := NewRouter(h, 1<<20)

	rec := do(srv, http.MethodPost, "/v1/chat/completions",
		`{"model":"mock","messages":[{"role":"user","content":"hi"}]}`, "Bearer "+testAPIKey)
	if rec.Code != http.StatusBadGateway {
		t.Errorf("a pinned provider has no fallback, status %d, want 502", rec.Code)
	}
	if rec.Header().Get("X-Route-Tried") != "" {
		t.Error("X-Route-Tried is only for the model auto")
	}

	rec = do(srv, http.MethodPost, "/v1/chat/completions",
		`{"model":"mock-b","messages":[{"role":"user","content":"hi"}]}`, "Bearer "+testAPIKey)
	if rec.Code != 200 || rec.Header().Get("X-Provider") != "mock-b" {
		t.Errorf("mock-b can be chosen directly: status %d, provider %q", rec.Code, rec.Header().Get("X-Provider"))
	}
}

// The provider that failed has a different price. We must charge the one that answered.
func TestAutoDoesNotChargeTheProviderThatFailed(t *testing.T) {
	reg := provider.Registry{
		"groq":   &statusProvider{name: "groq", code: 503},
		"mock-b": provider.NewMockNamed("mock-b", 0, 0),
	}
	targets, err := router.ParseTargets("groq/openai/gpt-oss-120b,mock-b", reg)
	if err != nil {
		t.Fatal(err)
	}
	strategy, _ := router.NewStrategy("priority")
	fb := &fakeBudget{status: budgetOK()}
	h := &Handler{Providers: reg, Tenants: newFakeStore(), AdminToken: testAdminToken,
		Router: router.New(targets, strategy), Budget: fb}

	rec := do(NewRouter(h, 1<<20), http.MethodPost, "/v1/chat/completions", autoBody, "Bearer "+testAPIKey)
	if rec.Code != 200 || rec.Header().Get("X-Provider") != "mock-b" {
		t.Fatalf("status %d, provider %q", rec.Code, rec.Header().Get("X-Provider"))
	}
	want := usage.CostUSD("mock-b", "", 0, 4)
	if len(fb.added) != 1 || fb.added[0] != want {
		t.Errorf("charged %v, want %v (the price of mock-b)", fb.added, want)
	}
}
