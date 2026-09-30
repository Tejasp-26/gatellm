package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// NewRouter creates the chi router and registers all routes.
// More routes are added in later phases.
func NewRouter(h *Handler) http.Handler {
	r := chi.NewRouter()

	r.Get("/healthz", h.Healthz)
	r.Get("/readyz", h.Readyz)

	return r
}
