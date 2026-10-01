package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"gatellm/internal/cache"
	"gatellm/internal/embed"
	"gatellm/internal/metrics"
	"gatellm/internal/provider"
	"gatellm/internal/router"
)

// Handler holds the things our handlers need (dependencies).
type Handler struct {
	DB         *pgxpool.Pool
	Redis      *redis.Client
	Providers  provider.Registry
	Tenants    TenantStore
	AdminToken string
	Limiter    RateLimiter      // nil = no rate limiting
	Budget     BudgetTracker    // nil = no monthly budget
	Router     AutoRouter       // nil = the model "auto" is not available
	Cache      cache.Cache      // nil = no caching
	Semantic   cache.Semantic   // nil = no semantic cache (needs Embedder too)
	Embedder   embed.Embedder   // turns a prompt into a vector
	Usage      UsageRecorder    // nil = usage events are not recorded
	Metrics    *metrics.Metrics // nil = no metrics (every call on it does nothing)

	MetricsToken string // if not empty, GET /metrics needs "Authorization: Bearer <this>"

	CacheAllowTemperature bool // also cache requests with temperature above 0 (or none)

	flights singleflight.Group // groups identical requests that arrive at the same time
}

// AutoRouter is what the handlers need from the router (the real one is router.Router).
type AutoRouter interface {
	Chat(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, router.Served, error)
	ChatStream(ctx context.Context, req *provider.ChatRequest) (<-chan provider.StreamChunk, router.Served, error)
}

// writeJSON is a small helper to send JSON responses.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeError sends errors in the same shape as OpenAI, so OpenAI clients understand them:
// {"error": {"message": "...", "type": "..."}}
func writeError(w http.ResponseWriter, status int, errType, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{"message": message, "type": errType},
	})
}

// decodeJSON reads the request body into v.
// On a problem it sends the error response itself and returns false.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "invalid_request_error", "request body is too large")
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid_request_error", "body is not valid JSON")
		return false
	}
	return true
}

// Healthz says "the process is alive". It checks nothing else,
// so the platform does not restart us just because a database is slow.
func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Readyz says "I can serve traffic": Postgres and Redis must both answer.
func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	checks := map[string]string{"postgres": "ok", "redis": "ok"}
	ready := true

	if err := h.DB.Ping(ctx); err != nil {
		checks["postgres"] = "down"
		ready = false
	}
	if err := h.Redis.Ping(ctx).Err(); err != nil {
		checks["redis"] = "down"
		ready = false
	}

	if !ready {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not ready", "checks": checks})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "checks": checks})
}
