package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"gatellm/internal/metrics"
	"gatellm/internal/router"
)

// knownMethods: any other HTTP method becomes "other", so a client cannot create
// thousands of label values by sending random methods.
var knownMethods = map[string]bool{
	http.MethodGet: true, http.MethodPost: true, http.MethodPut: true, http.MethodPatch: true,
	http.MethodDelete: true, http.MethodHead: true, http.MethodOptions: true,
}

// MetricsMiddleware counts every HTTP request and measures how long it takes.
// The label "path" is the ROUTE PATTERN ("/admin/tenants/{id}/keys"), never the real path,
// otherwise every id would be a new time series.
func MetricsMiddleware(m *metrics.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if m == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			m.HTTPStart()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			defer func() {
				pattern := "unmatched" // no route matched (404)
				if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
					pattern = rc.RoutePattern()
				}
				method := r.Method
				if !knownMethods[method] {
					method = "other"
				}
				m.HTTPDone(pattern, method, rec.status, time.Since(start))
			}()
			next.ServeHTTP(rec, r)
		})
	}
}

// noteFallback counts requests where the router tried more than one provider.
// err != nil means every provider failed ("to" is then "none").
func (h *Handler) noteFallback(sv router.Served, err error) {
	if len(sv.Tried) < 2 {
		return
	}
	to := sv.Provider
	if err != nil {
		to = "none"
	}
	h.Metrics.Fallback(sv.Tried[0], to)
}
