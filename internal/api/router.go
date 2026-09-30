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
	r.Use(Recoverer)
	r.Use(BodyLimit(maxBodyBytes))

	r.Get("/healthz", h.Healthz)
	r.Get("/readyz", h.Readyz)

	r.Post("/v1/chat/completions", h.ChatCompletions)

	return r
}
