package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"gatellm/internal/provider"
	"gatellm/internal/usage"
)

// BudgetTracker is what the handlers need from the monthly budget.
// The real one is usage.Budget (Redis). Tests use a fake.
type BudgetTracker interface {
	Check(ctx context.Context, tenantID string, budgetUSD float64) (usage.BudgetStatus, error)
	Add(ctx context.Context, tenantID string, usd float64) error
}

// checkBudget runs before the provider call.
// On a problem it writes the error response and returns false.
func (h *Handler) checkBudget(w http.ResponseWriter, r *http.Request) bool {
	tenant := TenantFrom(r.Context())
	if h.Budget == nil || tenant == nil {
		return true // budget tracking is switched off
	}

	st, err := h.Budget.Check(r.Context(), tenant.ID, tenant.MonthlyBudgetUSD)
	if err != nil {
		// Redis is down and we run in fail-closed mode.
		slog.Error("budget tracker unavailable, rejecting request",
			"request_id", RequestIDFrom(r.Context()), "tenant_id", tenant.ID, "err", err)
		writeError(w, http.StatusServiceUnavailable, "server_error",
			"the budget tracker is unavailable, please try again shortly")
		return false
	}
	if st.Degraded {
		slog.Warn("budget tracker unavailable, letting request pass (fail-open)",
			"request_id", RequestIDFrom(r.Context()), "tenant_id", tenant.ID)
		return true
	}

	w.Header().Set("X-Budget-Remaining-USD", usage.FormatUSD(st.RemainingUSD))
	if !st.Allowed {
		// 402 Payment Required: waiting a few seconds will not help, so we do not send Retry-After.
		writeError(w, http.StatusPaymentRequired, "insufficient_quota",
			"the monthly budget of this account is used up, it resets at the start of next month (UTC)")
		return false
	}
	return true
}

// costTokens returns the prompt and answer tokens that we charge for.
// Real numbers from the provider are best. Without them we estimate from the text.
func costTokens(u *provider.Usage, promptEstimate int64, answerChars int) (prompt, completion int) {
	if u != nil && u.TotalTokens > 0 {
		return u.PromptTokens, u.CompletionTokens
	}
	return int(promptEstimate), answerChars / 4
}

// recordSpend adds the cost of a finished request to the tenant's monthly total.
// ctx is only used for the tenant and the request id, we do not stop when the client has left.
func (h *Handler) recordSpend(ctx context.Context, providerName, model string, u *provider.Usage, promptEstimate int64, answerChars int) {
	tenant := TenantFrom(ctx)
	if h.Budget == nil || tenant == nil {
		return
	}
	prompt, completion := costTokens(u, promptEstimate, answerChars)
	cost := usage.CostUSD(providerName, model, prompt, completion)

	addCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := h.Budget.Add(addCtx, tenant.ID, cost); err != nil {
		// The money is not counted. Phase 7 (usage events in Postgres) will keep the true record.
		slog.Warn("could not add cost to the monthly budget",
			"request_id", RequestIDFrom(ctx), "tenant_id", tenant.ID, "cost_usd", cost, "err", err)
	}
}
