// Package metrics defines what the gateway shows to Prometheus at GET /metrics.
//
// Prometheus is a tool that visits /metrics every few seconds and stores the numbers, so you can
// draw graphs and set alarms. There are three kinds of numbers:
//
//	counter    only goes up (requests, tokens, dollars). You graph its rate per second.
//	gauge      goes up and down (circuit breaker state, requests running right now).
//	histogram  counts how many values fell in each "bucket", so you can read p50, p95 and p99.
//
// Every method works on a nil *Metrics and then does nothing. So tests and a gateway with
// metrics switched off need no special code.
//
// IMPORTANT: a label value must come from a SMALL fixed list (provider, status, route pattern).
// Never use a request id, a user id or a prompt as a label: every new value creates a new time
// series in Prometheus and a huge number of them makes it crash.
package metrics

import (
	"crypto/subtle"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"gatellm/internal/usage"
)

// secondsBuckets suit LLM calls: from 5 ms (cache hit) up to 60 s (slow answer).
var secondsBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

type Metrics struct {
	reg *prometheus.Registry

	httpRequests *prometheus.CounterVec   // path, method, code
	httpDuration *prometheus.HistogramVec // path, method
	inFlight     prometheus.Gauge

	chatRequests *prometheus.CounterVec   // provider, cache, status
	chatDuration *prometheus.HistogramVec // provider, cache
	cacheResults *prometheus.CounterVec   // result
	tokens       *prometheus.CounterVec   // provider, type
	costUSD      *prometheus.CounterVec   // provider
	rejections   *prometheus.CounterVec   // reason
	fallbacks    *prometheus.CounterVec   // from, to
	retries      *prometheus.CounterVec   // provider
	breakerMoves *prometheus.CounterVec   // provider, to
	firstToken   *prometheus.HistogramVec // provider
}

// New creates the metrics with their own registry (so tests do not share global state).
func New() *Metrics {
	m := &Metrics{reg: prometheus.NewRegistry()}
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "gatellm", Name: name, Help: help}, labels)
		m.reg.MustRegister(c)
		return c
	}
	histogram := func(name, help string, labels ...string) *prometheus.HistogramVec {
		h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "gatellm", Name: name, Help: help, Buckets: secondsBuckets}, labels)
		m.reg.MustRegister(h)
		return h
	}

	m.httpRequests = counter("http_requests_total", "HTTP requests by route, method and status code.", "path", "method", "code")
	m.httpDuration = histogram("http_request_duration_seconds", "How long HTTP requests take (a stream counts until it ends).", "path", "method")
	m.inFlight = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "gatellm", Name: "http_in_flight_requests", Help: "Requests being handled right now."})
	m.reg.MustRegister(m.inFlight)

	m.chatRequests = counter("chat_requests_total", "Chat requests that reached a provider or the cache, by provider, cache status and outcome.", "provider", "cache", "status")
	m.chatDuration = histogram("chat_request_duration_seconds", "Chat request time from the first byte to the end, by provider and cache status.", "provider", "cache")
	m.cacheResults = counter("cache_results_total", "Cache outcome of every chat request: HIT-EXACT, HIT-SEMANTIC, MISS, COALESCED, BYPASS or OFF.", "result")
	m.tokens = counter("tokens_total", "Tokens used, by provider and type (prompt or completion). Cache hits use none.", "provider", "type")
	m.costUSD = counter("cost_usd_total", "Money spent on providers in US dollars (the price table in the code).", "provider")
	m.rejections = counter("rejections_total", "Requests we refused: rpm, tpm, budget, limiter_down or budget_down.", "reason")
	m.fallbacks = counter("fallbacks_total", "Times the router had to try another provider. to=none means every provider failed.", "from", "to")
	m.retries = counter("provider_retries_total", "Retries of a provider call after a temporary failure.", "provider")
	m.breakerMoves = counter("circuit_breaker_transitions_total", "Circuit breaker state changes, by provider and new state.", "provider", "to")
	m.firstToken = histogram("stream_first_token_seconds", "Time until the first piece of text of a stream (time to first token).", "provider")

	// Standard numbers about the Go program: memory, goroutines, garbage collection, open files.
	m.reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// Registry is for tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Handler serves GET /metrics. If token is not empty, the caller must send "Authorization: Bearer <token>"
// (Prometheus can do that with its `authorization` setting).
func (m *Metrics) Handler(token string) http.Handler {
	inner := promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
	if token == "" {
		return inner
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		inner.ServeHTTP(w, r)
	})
}

// ---- called by the code of the gateway ----

// HTTPStart / HTTPDone wrap every request (see the middleware in the api package).
func (m *Metrics) HTTPStart() {
	if m != nil {
		m.inFlight.Inc()
	}
}

func (m *Metrics) HTTPDone(path, method string, code int, d time.Duration) {
	if m == nil {
		return
	}
	m.inFlight.Dec()
	m.httpRequests.WithLabelValues(path, method, strconv.Itoa(code)).Inc()
	m.httpDuration.WithLabelValues(path, method).Observe(d.Seconds())
}

// Chat records one finished chat request.
func (m *Metrics) Chat(providerName, cacheStatus, status string, promptTokens, completionTokens int, costUSD float64, d time.Duration) {
	if m == nil {
		return
	}
	if providerName == "" {
		providerName = "none" // e.g. a request that failed before any provider was chosen
	}
	m.chatRequests.WithLabelValues(providerName, cacheStatus, status).Inc()
	m.chatDuration.WithLabelValues(providerName, cacheStatus).Observe(d.Seconds())
	m.cacheResults.WithLabelValues(cacheStatus).Inc()
	if promptTokens > 0 {
		m.tokens.WithLabelValues(providerName, "prompt").Add(float64(promptTokens))
	}
	if completionTokens > 0 {
		m.tokens.WithLabelValues(providerName, "completion").Add(float64(completionTokens))
	}
	if costUSD > 0 {
		m.costUSD.WithLabelValues(providerName).Add(costUSD)
	}
}

// Rejected counts a request we refused. reason: rpm, tpm, budget, limiter_down, budget_down.
func (m *Metrics) Rejected(reason string) {
	if m != nil {
		m.rejections.WithLabelValues(reason).Inc()
	}
}

// Fallback counts a request where the router needed more than one provider.
func (m *Metrics) Fallback(from, to string) {
	if m != nil {
		m.fallbacks.WithLabelValues(from, to).Inc()
	}
}

func (m *Metrics) Retry(providerName string) {
	if m != nil {
		m.retries.WithLabelValues(providerName).Inc()
	}
}

func (m *Metrics) BreakerChanged(providerName, to string) {
	if m != nil {
		m.breakerMoves.WithLabelValues(providerName, to).Inc()
	}
}

func (m *Metrics) FirstToken(providerName string, d time.Duration) {
	if m != nil {
		m.firstToken.WithLabelValues(providerName).Observe(d.Seconds())
	}
}

// WatchBreaker shows the circuit breaker of one provider: 0 = closed (healthy), 1 = open (blocked), 2 = half-open (testing).
// The function is called when Prometheus reads /metrics, so the number is always current.
func (m *Metrics) WatchBreaker(providerName string, state func() int) {
	if m == nil {
		return
	}
	m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "gatellm", Name: "circuit_breaker_state",
		Help:        "Circuit breaker of a provider: 0 closed, 1 open, 2 half-open.",
		ConstLabels: prometheus.Labels{"provider": providerName},
	}, func() float64 { return float64(state()) }))
}

// WatchUsage shows the counters of the usage pipeline.
func (m *Metrics) WatchUsage(c *usage.Counters) {
	if m == nil {
		return
	}
	for name, f := range map[string]struct {
		help string
		fn   func() int64
	}{
		"usage_events_published_total": {"Usage events put in the Redis Stream.", c.Published},
		"usage_events_fallback_total":  {"Usage events written straight to Postgres because the stream failed.", c.Fallback},
		"usage_events_lost_total":      {"Usage events that could not be saved anywhere (alarm if above 0). They are in the error log.", c.Lost},
		"usage_events_written_total":   {"Usage events the consumer wrote to Postgres.", c.Written},
		"usage_events_dead_total":      {"Usage events moved to the dead-letter stream (need a human).", c.Dead},
	} {
		fn := f.fn
		m.reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "gatellm", Name: name, Help: f.help},
			func() float64 { return float64(fn()) }))
	}
}
