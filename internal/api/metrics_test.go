package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"gatellm/internal/metrics"
	"gatellm/internal/provider"
	"gatellm/internal/ratelimit"
	"gatellm/internal/router"
	"gatellm/internal/usage"
)

// value reads one number from the registry. want is "label=value" pairs; 0 if the series does not exist.
func value(t *testing.T, m *metrics.Metrics, name string, want ...string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, mt := range f.GetMetric() {
			if labelsMatch(mt.GetLabel(), want) {
				switch {
				case mt.Counter != nil:
					return mt.GetCounter().GetValue()
				case mt.Gauge != nil:
					return mt.GetGauge().GetValue()
				case mt.Histogram != nil:
					return float64(mt.GetHistogram().GetSampleCount())
				}
			}
		}
	}
	return 0
}

func labelsMatch(have []*dto.LabelPair, want []string) bool {
	for _, w := range want {
		ok := false
		for _, l := range have {
			if l.GetName()+"="+l.GetValue() == w {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func metricsServer(m *metrics.Metrics, p provider.Provider, mod func(*Handler)) http.Handler {
	h := &Handler{
		Providers: provider.Registry{"mock": p}, Tenants: newFakeStore(), AdminToken: testAdminToken,
		Metrics: m, Budget: &fakeBudget{status: budgetOK()},
	}
	if mod != nil {
		mod(h)
	}
	return NewRouter(h, 1<<20)
}

func TestMetricsEndpointIsServed(t *testing.T) {
	m := metrics.New()
	srv := metricsServer(m, provider.NewMock(0, 0), nil)
	rec := do(srv, http.MethodGet, "/metrics", "", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "go_goroutines") {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestMetricsEndpointNeedsTokenWhenSet(t *testing.T) {
	m := metrics.New()
	srv := metricsServer(m, provider.NewMock(0, 0), func(h *Handler) { h.MetricsToken = "s3" })
	if rec := do(srv, http.MethodGet, "/metrics", "", ""); rec.Code != 401 {
		t.Errorf("no token: %d", rec.Code)
	}
	if rec := do(srv, http.MethodGet, "/metrics", "", "Bearer s3"); rec.Code != 200 {
		t.Errorf("right token: %d", rec.Code)
	}
}

func TestNoMetricsEndpointWhenMetricsAreOff(t *testing.T) {
	srv := metricsServer(nil, provider.NewMock(0, 0), nil)
	if rec := do(srv, http.MethodGet, "/metrics", "", ""); rec.Code != 404 {
		t.Errorf("status %d, want 404", rec.Code)
	}
}

func TestHTTPMetricsUseRoutePatternAndStatus(t *testing.T) {
	m := metrics.New()
	srv := metricsServer(m, provider.NewMock(0, 0), nil)
	do(srv, http.MethodGet, "/healthz", "", "")
	do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "") // no key -> 401
	do(srv, http.MethodGet, "/no/such/thing/123", "", "")

	if value(t, m, "gatellm_http_requests_total", "path=/healthz", "code=200") != 1 {
		t.Error("healthz not counted")
	}
	if value(t, m, "gatellm_http_requests_total", "path=/v1/chat/completions", "code=401") != 1 {
		t.Error("401 not counted")
	}
	if value(t, m, "gatellm_http_requests_total", "path=unmatched", "code=404") != 1 {
		t.Error("the real path must not become a label; expected path=unmatched")
	}
}

func TestStrangeHTTPMethodBecomesOther(t *testing.T) {
	m := metrics.New()
	srv := metricsServer(m, provider.NewMock(0, 0), nil)
	req := httptest.NewRequest("BREW", "/healthz", nil)
	srv.ServeHTTP(httptest.NewRecorder(), req)
	if value(t, m, "gatellm_http_requests_total", "method=other") != 1 {
		t.Error("unknown methods must be counted as method=other")
	}
}

func TestInFlightReturnsToZero(t *testing.T) {
	m := metrics.New()
	srv := metricsServer(m, provider.NewMock(0, 0), nil)
	do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	do(srv, http.MethodGet, "/healthz", "", "")
	if got := value(t, m, "gatellm_http_in_flight_requests"); got != 0 {
		t.Errorf("in flight = %v", got)
	}
}

func TestChatMetricsMatchTheUsageEvent(t *testing.T) {
	m := metrics.New()
	fu := &fakeUsage{}
	srv := metricsServer(m, provider.NewMock(0, 0), func(h *Handler) { h.Usage = fu })
	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	ev := fu.one(t)
	if value(t, m, "gatellm_chat_requests_total", "provider=mock", "cache=OFF", "status=ok") != 1 {
		t.Error("chat request not counted")
	}
	if got := value(t, m, "gatellm_tokens_total", "provider=mock", "type=completion"); got != float64(ev.CompletionTokens) {
		t.Errorf("completion tokens metric %v, event %d", got, ev.CompletionTokens)
	}
	if got := value(t, m, "gatellm_tokens_total", "provider=mock", "type=prompt"); got != float64(ev.PromptTokens) {
		t.Errorf("prompt tokens metric %v, event %d", got, ev.PromptTokens)
	}
	if got := value(t, m, "gatellm_cost_usd_total", "provider=mock"); got != ev.CostUSD {
		t.Errorf("cost metric %v, event %v", got, ev.CostUSD)
	}
	if value(t, m, "gatellm_chat_request_duration_seconds", "provider=mock") != 1 {
		t.Error("duration not observed")
	}
}

func TestMetricsWorkWithoutUsageRecorder(t *testing.T) {
	m := metrics.New()
	srv := metricsServer(m, provider.NewMock(0, 0), nil) // h.Usage == nil
	do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if value(t, m, "gatellm_chat_requests_total", "status=ok") != 1 {
		t.Error("metrics must not depend on the usage pipeline")
	}
}

func TestCacheMetricsMissHitBypass(t *testing.T) {
	m := metrics.New()
	srv := metricsServer(m, provider.NewMock(0, 0), func(h *Handler) { h.Cache = newMemCache() })
	body := `{"model":"mock","temperature":0,"messages":[{"role":"user","content":"same"}]}`
	do(srv, http.MethodPost, "/v1/chat/completions", body, "Bearer "+testAPIKey)
	do(srv, http.MethodPost, "/v1/chat/completions", body, "Bearer "+testAPIKey)
	do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey) // no temperature -> bypass

	for result, want := range map[string]float64{"MISS": 1, "HIT-EXACT": 1, "BYPASS": 1} {
		if got := value(t, m, "gatellm_cache_results_total", "result="+result); got != want {
			t.Errorf("cache result %s = %v, want %v", result, got, want)
		}
	}
	// A hit costs nothing: only the one MISS used tokens.
	if got := value(t, m, "gatellm_tokens_total", "type=completion"); got != 8 {
		t.Errorf("completion tokens = %v, want 8 (2 real calls x 4)", got)
	}
}

func TestRejectionMetrics(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason string
		mod    func(*Handler)
	}{
		{"rpm", "rpm", func(h *Handler) {
			h.Limiter = &fakeLimiter{decision: ratelimit.Decision{Reason: "rpm", RetryAfter: 1e9}}
		}},
		{"tpm", "tpm", func(h *Handler) {
			h.Limiter = &fakeLimiter{decision: ratelimit.Decision{Reason: "tpm", RetryAfter: 1e9}}
		}},
		{"limiter down", "limiter_down", func(h *Handler) {
			h.Limiter = &fakeLimiter{err: ratelimit.ErrUnavailable}
		}},
		{"budget", "budget", func(h *Handler) {
			h.Budget = &fakeBudget{status: usage.BudgetStatus{Allowed: false, SpentUSD: 5}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := metrics.New()
			srv := metricsServer(m, provider.NewMock(0, 0), tc.mod)
			rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
			if rec.Code == 200 {
				t.Fatal("request should have been refused")
			}
			if got := value(t, m, "gatellm_rejections_total", "reason="+tc.reason); got != 1 {
				t.Errorf("rejections{%s} = %v, want 1", tc.reason, got)
			}
		})
	}
}

func TestFailedProviderCallIsCountedAsError(t *testing.T) {
	m := metrics.New()
	srv := metricsServer(m, provider.NewMock(0, 1), nil)
	rec := do(srv, http.MethodPost, "/v1/chat/completions", hiBody, "Bearer "+testAPIKey)
	if rec.Code == 200 {
		t.Fatal("expected an error")
	}
	if value(t, m, "gatellm_chat_requests_total", "provider=mock", "status=error") != 1 {
		t.Error("error not counted")
	}
	if value(t, m, "gatellm_cost_usd_total") != 0 {
		t.Error("a failed call must cost nothing")
	}
}

func TestFallbackIsCounted(t *testing.T) {
	m := metrics.New()
	a, b := provider.NewMock(0, 1), provider.NewMockNamed("mock-b", 0, 0)
	reg := provider.Registry{"mock": a, "mock-b": b}
	targets, _ := router.ParseTargets("mock,mock-b", reg)
	strategy, _ := router.NewStrategy("priority")
	h := &Handler{Providers: reg, Tenants: newFakeStore(), AdminToken: testAdminToken,
		Router: router.New(targets, strategy), Metrics: m}
	rec := do(NewRouter(h, 1<<20), http.MethodPost, "/v1/chat/completions", autoBody, "Bearer "+testAPIKey)
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	if value(t, m, "gatellm_fallbacks_total", "from=mock", "to=mock-b") != 1 {
		t.Error("fallback mock -> mock-b not counted")
	}
	if value(t, m, "gatellm_chat_requests_total", "provider=mock-b", "status=ok") != 1 {
		t.Error("the answering provider must be mock-b")
	}
}

func TestNoFallbackMetricWhenFirstProviderWorks(t *testing.T) {
	m := metrics.New()
	reg := provider.Registry{"mock": provider.NewMock(0, 0), "mock-b": provider.NewMockNamed("mock-b", 0, 0)}
	targets, _ := router.ParseTargets("mock,mock-b", reg)
	strategy, _ := router.NewStrategy("priority")
	h := &Handler{Providers: reg, Tenants: newFakeStore(), AdminToken: testAdminToken,
		Router: router.New(targets, strategy), Metrics: m}
	do(NewRouter(h, 1<<20), http.MethodPost, "/v1/chat/completions", autoBody, "Bearer "+testAPIKey)
	if value(t, m, "gatellm_fallbacks_total") != 0 {
		t.Error("no fallback should be counted")
	}
}

func TestAllProvidersFailingIsCountedAsFallbackToNone(t *testing.T) {
	m := metrics.New()
	reg := provider.Registry{"mock": provider.NewMock(0, 1), "mock-b": provider.NewMockNamed("mock-b", 0, 1)}
	targets, _ := router.ParseTargets("mock,mock-b", reg)
	strategy, _ := router.NewStrategy("priority")
	h := &Handler{Providers: reg, Tenants: newFakeStore(), AdminToken: testAdminToken,
		Router: router.New(targets, strategy), Metrics: m}
	rec := do(NewRouter(h, 1<<20), http.MethodPost, "/v1/chat/completions", autoBody, "Bearer "+testAPIKey)
	if rec.Code == 200 {
		t.Fatal("expected failure")
	}
	if value(t, m, "gatellm_fallbacks_total", "from=mock", "to=none") != 1 {
		t.Error("total failure must be counted as to=none")
	}
}

func TestStreamMetrics(t *testing.T) {
	m := metrics.New()
	srv := metricsServer(m, provider.NewMock(0, 0), nil)
	rec := streamPost(srv, `{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "[DONE]") {
		t.Fatalf("stream broken by the middleware: %d", rec.Code)
	}
	if value(t, m, "gatellm_stream_first_token_seconds", "provider=mock") != 1 {
		t.Error("time to first token not recorded")
	}
	if value(t, m, "gatellm_chat_requests_total", "provider=mock", "status=ok") != 1 {
		t.Error("stream not counted")
	}
	if value(t, m, "gatellm_http_requests_total", "path=/v1/chat/completions", "code=200") != 1 {
		t.Error("stream HTTP request not counted")
	}
}
