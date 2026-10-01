package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// NewRouter creates the chi router and registers all routes.
// More routes are added in later phases.
func NewRouter(h *Handler, maxBodyBytes int64) http.Handler {
	r := chi.NewRouter()

	// Middleware runs top to bottom for every request.
	// Logger is outside Recoverer, so a panic is logged as a 500.
	r.Use(RequestID)
	r.Use(Logger)
	r.Use(MetricsMiddleware(h.Metrics)) // outside Recoverer, so a panic is counted as a 500
	r.Use(Recoverer)
	r.Use(BodyLimit(maxBodyBytes))

	// Open to everyone.
	r.Get("/healthz", h.Healthz)
	r.Get("/readyz", h.Readyz)

	// Prometheus visits this page. Set METRICS_TOKEN to protect it.
	if h.Metrics != nil {
		r.Method(http.MethodGet, "/metrics", h.Metrics.Handler(h.MetricsToken))
	}

	// Client API: needs a tenant API key.
	r.With(h.Auth).Post("/v1/chat/completions", h.ChatCompletions)

	// Admin API: needs the ADMIN_TOKEN.
	r.Route("/admin", func(r chi.Router) {
		r.Use(h.AdminAuth)
		r.Post("/tenants", h.CreateTenant)
		r.Post("/tenants/{id}/keys", h.AddTenantKey)
	})

	return r
}
