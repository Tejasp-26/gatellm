package api

import (
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strings"

	"github.com/go-chi/chi/v5"

	"gatellm/internal/store"
)

// Default limits for a new tenant (same as the table defaults).
const (
	defaultRPM    = 60
	defaultTPM    = 20000
	defaultBudget = 5.0
)

const keyWarning = "Save this API key now. It is shown only once and cannot be recovered."

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// The limits are pointers, so we can tell "not sent" (use default) from "sent as 0".
type createTenantRequest struct {
	Name             string   `json:"name"`
	RPMLimit         *int     `json:"rpm_limit"`
	TPMLimit         *int     `json:"tpm_limit"`
	MonthlyBudgetUSD *float64 `json:"monthly_budget_usd"`
}

// CreateTenant handles POST /admin/tenants.
// It creates a tenant and its first API key, and shows the raw key once.
func (h *Handler) CreateTenant(w http.ResponseWriter, r *http.Request) {
	var req createTenantRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" || len(name) > 100 {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "name is required (max 100 characters)")
		return
	}
	rpm, tpm, budget := defaultRPM, defaultTPM, defaultBudget
	if req.RPMLimit != nil {
		rpm = *req.RPMLimit
	}
	if req.TPMLimit != nil {
		tpm = *req.TPMLimit
	}
	if req.MonthlyBudgetUSD != nil {
		budget = *req.MonthlyBudgetUSD
	}
	if rpm <= 0 || tpm <= 0 || budget < 0 {
		writeError(w, http.StatusBadRequest, "invalid_request_error",
			"rpm_limit and tpm_limit must be above 0, monthly_budget_usd must not be negative")
		return
	}

	rawKey, keyHash, err := generateKey()
	if err != nil {
		slog.Error("generate key failed", "err", err)
		writeError(w, http.StatusInternalServerError, "server_error", "internal server error")
		return
	}

	tenant, err := h.Tenants.CreateTenant(r.Context(), name, rpm, tpm, budget, keyHash)
	if err != nil {
		slog.Error("create tenant failed", "request_id", RequestIDFrom(r.Context()), "err", err)
		writeError(w, http.StatusInternalServerError, "server_error", "could not create tenant")
		return
	}
	slog.Info("tenant created", "request_id", RequestIDFrom(r.Context()), "tenant_id", tenant.ID) // never log the key

	w.Header().Set("Cache-Control", "no-store") // the response contains a secret
	writeJSON(w, http.StatusCreated, map[string]any{
		"tenant":  tenant,
		"api_key": rawKey,
		"warning": keyWarning,
	})
}

// AddTenantKey handles POST /admin/tenants/{id}/keys.
// It creates one more API key for an existing tenant.
func (h *Handler) AddTenantKey(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "id")
	if !uuidPattern.MatchString(tenantID) {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "tenant id must be a UUID")
		return
	}

	rawKey, keyHash, err := generateKey()
	if err != nil {
		slog.Error("generate key failed", "err", err)
		writeError(w, http.StatusInternalServerError, "server_error", "internal server error")
		return
	}

	err = h.Tenants.AddKey(r.Context(), tenantID, keyHash)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "invalid_request_error", "tenant not found")
		return
	}
	if err != nil {
		slog.Error("add key failed", "request_id", RequestIDFrom(r.Context()), "err", err)
		writeError(w, http.StatusInternalServerError, "server_error", "could not create key")
		return
	}
	slog.Info("api key created", "request_id", RequestIDFrom(r.Context()), "tenant_id", tenantID)

	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusCreated, map[string]any{
		"tenant_id": tenantID,
		"api_key":   rawKey,
		"warning":   keyWarning,
	})
}
