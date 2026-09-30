package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"gatellm/internal/store"
)

// TenantStore is what the API needs from the database for tenants and keys.
// It is an interface only so that tests can use a simple in-memory fake
// instead of a real database. The real one is store.Tenants.
type TenantStore interface {
	CreateTenant(ctx context.Context, name string, rpm, tpm int, budget float64, keyHash string) (*store.Tenant, error)
	AddKey(ctx context.Context, tenantID, keyHash string) error
	FindByKeyHash(ctx context.Context, keyHash string) (*store.Tenant, error)
}

const tenantCtxKey ctxKey = "tenant"

// TenantFrom returns the tenant that Auth stored in the context (nil if there is none).
func TenantFrom(ctx context.Context) *store.Tenant {
	t, _ := ctx.Value(tenantCtxKey).(*store.Tenant)
	return t
}

// tenantIDFrom is a small helper for logs.
func tenantIDFrom(ctx context.Context) string {
	if t := TenantFrom(ctx); t != nil {
		return t.ID
	}
	return ""
}

// bearerToken reads the token from the header "Authorization: Bearer <token>".
func bearerToken(r *http.Request) (string, bool) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	token = strings.TrimSpace(token)
	return token, ok && token != ""
}

// Auth checks the customer's API key. If it is valid, the tenant goes into the request context.
func (h *Handler) Auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := bearerToken(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "authentication_error",
				"missing API key, send the header: Authorization: Bearer <key>")
			return
		}

		// We never store the raw key, so we look up its hash.
		tenant, err := h.Tenants.FindByKeyHash(r.Context(), hashKey(key))
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusUnauthorized, "authentication_error", "invalid API key")
			return
		}
		if err != nil {
			slog.Error("api key lookup failed", "request_id", RequestIDFrom(r.Context()), "err", err)
			writeError(w, http.StatusInternalServerError, "server_error", "internal server error")
			return
		}

		ctx := context.WithValue(r.Context(), tenantCtxKey, tenant)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// AdminAuth protects the /admin endpoints with the ADMIN_TOKEN.
func (h *Handler) AdminAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// If no admin token is configured, the admin API stays closed.
		// (Otherwise an empty token could match an empty header.)
		if h.AdminToken == "" {
			writeError(w, http.StatusServiceUnavailable, "server_error", "admin API is disabled: ADMIN_TOKEN is not set")
			return
		}

		token, ok := bearerToken(r)

		// Compare the hashes in constant time, so response time does not leak how much matched.
		got := sha256.Sum256([]byte(token))
		want := sha256.Sum256([]byte(h.AdminToken))
		if !ok || subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			writeError(w, http.StatusUnauthorized, "authentication_error", "invalid admin token")
			return
		}
		next.ServeHTTP(w, r)
	})
}
