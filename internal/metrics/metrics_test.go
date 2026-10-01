package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"gatellm/internal/usage"
)

// scrape calls the /metrics handler like Prometheus would.
func scrape(m *Metrics, token, auth string) (int, string) {
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	m.Handler(token).ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func TestNilMetricsDoNothing(t *testing.T) {
	var m *Metrics // every call must be safe
	m.HTTPStart()
	m.HTTPDone("/x", "GET", 200, time.Second)
	m.Chat("mock", "MISS", "ok", 1, 1, 0.1, time.Second)
	m.Rejected("rpm")
	m.Fallback("a", "b")
	m.Retry("a")
	m.BreakerChanged("a", "open")
	m.FirstToken("a", time.Second)
	m.WatchBreaker("a", func() int { return 0 })
	m.WatchUsage(&usage.Counters{})
}

func TestChatCountsTokensCostAndCache(t *testing.T) {
	m := New()
	m.Chat("mock", "MISS", "ok", 10, 4, 0.5, 20*time.Millisecond)
	m.Chat("mock", "HIT-EXACT", "ok", 0, 0, 0, time.Millisecond)

	if got := testutil.ToFloat64(m.chatRequests.WithLabelValues("mock", "MISS", "ok")); got != 1 {
		t.Errorf("chat requests MISS = %v", got)
	}
	if got := testutil.ToFloat64(m.tokens.WithLabelValues("mock", "prompt")); got != 10 {
		t.Errorf("prompt tokens = %v", got)
	}
	if got := testutil.ToFloat64(m.tokens.WithLabelValues("mock", "completion")); got != 4 {
		t.Errorf("completion tokens = %v", got)
	}
	if got := testutil.ToFloat64(m.costUSD.WithLabelValues("mock")); got != 0.5 {
		t.Errorf("cost = %v", got)
	}
	if testutil.ToFloat64(m.cacheResults.WithLabelValues("MISS")) != 1 || testutil.ToFloat64(m.cacheResults.WithLabelValues("HIT-EXACT")) != 1 {
		t.Error("cache results are wrong")
	}
}

func TestEmptyProviderBecomesNone(t *testing.T) {
	m := New()
	m.Chat("", "OFF", "error", 0, 0, 0, time.Millisecond)
	if testutil.ToFloat64(m.chatRequests.WithLabelValues("none", "OFF", "error")) != 1 {
		t.Error("a request without provider must be counted as provider=none")
	}
}

func TestHTTPInFlightGoesUpAndDown(t *testing.T) {
	m := New()
	m.HTTPStart()
	m.HTTPStart()
	if got := testutil.ToFloat64(m.inFlight); got != 2 {
		t.Fatalf("in flight = %v, want 2", got)
	}
	m.HTTPDone("/a", "GET", 200, time.Millisecond)
	m.HTTPDone("/a", "GET", 500, time.Millisecond)
	if got := testutil.ToFloat64(m.inFlight); got != 0 {
		t.Errorf("in flight = %v, want 0", got)
	}
	if testutil.ToFloat64(m.httpRequests.WithLabelValues("/a", "GET", "500")) != 1 {
		t.Error("the 500 was not counted")
	}
}

func TestSmallCounters(t *testing.T) {
	m := New()
	m.Rejected("rpm")
	m.Rejected("rpm")
	m.Fallback("mock", "mock-b")
	m.Retry("groq")
	m.BreakerChanged("groq", "open")
	m.FirstToken("mock", 30*time.Millisecond)
	if testutil.ToFloat64(m.rejections.WithLabelValues("rpm")) != 2 ||
		testutil.ToFloat64(m.fallbacks.WithLabelValues("mock", "mock-b")) != 1 ||
		testutil.ToFloat64(m.retries.WithLabelValues("groq")) != 1 ||
		testutil.ToFloat64(m.breakerMoves.WithLabelValues("groq", "open")) != 1 {
		t.Error("a small counter has a wrong value")
	}
	if testutil.CollectAndCount(m.firstToken) != 1 {
		t.Error("first token histogram has no sample")
	}
}

func TestBreakerGaugeFollowsTheFunction(t *testing.T) {
	m := New()
	state := 0
	m.WatchBreaker("groq", func() int { return state })
	_, body := scrape(m, "", "")
	if !strings.Contains(body, `gatellm_circuit_breaker_state{provider="groq"} 0`) {
		t.Errorf("closed breaker not shown:\n%s", grep(body, "circuit_breaker_state"))
	}
	state = 1
	_, body = scrape(m, "", "")
	if !strings.Contains(body, `gatellm_circuit_breaker_state{provider="groq"} 1`) {
		t.Error("open breaker not shown")
	}
}

func TestUsageCountersAreShown(t *testing.T) {
	m := New()
	c := &usage.Counters{}
	m.WatchUsage(c)
	_, body := scrape(m, "", "")
	if !strings.Contains(body, "gatellm_usage_events_lost_total 0") {
		t.Error("lost counter missing")
	}
}

func TestMetricsEndpointFormatAndStandardCollectors(t *testing.T) {
	m := New()
	m.Rejected("budget")
	code, body := scrape(m, "", "")
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	for _, want := range []string{`gatellm_rejections_total{reason="budget"} 1`, "go_goroutines", "# HELP gatellm_rejections_total"} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in /metrics", want)
		}
	}
}

func TestMetricsTokenProtection(t *testing.T) {
	m := New()
	for _, tc := range []struct {
		name, auth string
		want       int
	}{
		{"no header", "", 401},
		{"wrong token", "Bearer nope", 401},
		{"wrong scheme", "Basic secret", 401},
		{"right token", "Bearer secret", 200},
	} {
		if code, _ := scrape(m, "secret", tc.auth); code != tc.want {
			t.Errorf("%s: status %d, want %d", tc.name, code, tc.want)
		}
	}
}

func grep(body, word string) string {
	var out []string
	for _, l := range strings.Split(body, "\n") {
		if strings.Contains(l, word) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
