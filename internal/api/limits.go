package api

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"gatellm/internal/provider"
	"gatellm/internal/ratelimit"
)

// RateLimiter is what the handlers need from the rate limiter.
// The real one is ratelimit.Limiter (Redis). Tests use a fake.
type RateLimiter interface {
	Allow(ctx context.Context, tenantID string, lim ratelimit.Limits, cost int64) (ratelimit.Decision, error)
	Adjust(ctx context.Context, tenantID string, lim ratelimit.Limits, delta int64) error
}

// defaultMaxTokens is our guess for the answer length when the client sets no max_tokens.
const defaultMaxTokens = 256

// estimatePromptTokens guesses the input size: about 4 characters per token.
func estimatePromptTokens(req *provider.ChatRequest) int64 {
	var chars int
	for _, m := range req.Messages {
		chars += utf8.RuneCountInString(m.Content)
	}
	return int64(chars / 4)
}

// estimateTokens is the amount we reserve before the call: prompt + expected answer.
func estimateTokens(req *provider.ChatRequest) int64 {
	answer := int64(req.MaxTokens)
	if answer <= 0 {
		answer = defaultMaxTokens
	}
	return estimatePromptTokens(req) + answer
}

// actualTokens is what we really used. Real numbers from the provider are best.
// If the provider gave none, we estimate again from the text we received.
func actualTokens(usage *provider.Usage, promptEstimate int64, answerChars int) int64 {
	if usage != nil && usage.TotalTokens > 0 {
		return int64(usage.TotalTokens)
	}
	return promptEstimate + int64(answerChars/4)
}

// reservation remembers what we reserved for one request, so we can fix the number afterwards.
// A nil *reservation means "no limiting", and all its methods then do nothing.
type reservation struct {
	h              *Handler
	tenantID       string
	limits         ratelimit.Limits
	estimate       int64 // tokens we took before the call
	promptEstimate int64
	ctx            context.Context // only used for request_id in logs and to survive cancellation
}

// reserve checks the tenant's limits. On a problem it writes the error response and returns ok=false.
func (h *Handler) reserve(w http.ResponseWriter, r *http.Request, req *provider.ChatRequest) (*reservation, bool) {
	tenant := TenantFrom(r.Context())
	if h.Limiter == nil || tenant == nil {
		return nil, true // limiting is switched off
	}
	lim := ratelimit.Limits{RPM: tenant.RPMLimit, TPM: tenant.TPMLimit}
	estimate := estimateTokens(req)

	// A request bigger than a whole minute of tokens could never pass. Tell the client now.
	if estimate > int64(lim.TPM) {
		writeError(w, http.StatusBadRequest, "invalid_request_error",
			"this request needs about "+strconv.FormatInt(estimate, 10)+" tokens, but your limit is "+
				strconv.Itoa(lim.TPM)+" tokens per minute. Shorten the messages or lower max_tokens")
		return nil, false
	}

	d, err := h.Limiter.Allow(r.Context(), tenant.ID, lim, estimate)
	if err != nil {
		// Redis is down and we run in fail-closed mode.
		slog.Error("rate limiter unavailable, rejecting request",
			"request_id", RequestIDFrom(r.Context()), "tenant_id", tenant.ID, "err", err)
		writeError(w, http.StatusServiceUnavailable, "server_error",
			"the rate limiter is unavailable, please try again shortly")
		return nil, false
	}
	if d.Degraded {
		slog.Warn("rate limiter unavailable, letting request pass (fail-open)",
			"request_id", RequestIDFrom(r.Context()), "tenant_id", tenant.ID)
	} else {
		setRateLimitHeaders(w, lim, d)
	}

	if !d.Allowed {
		w.Header().Set("Retry-After", strconv.Itoa(ratelimit.RetryAfterSeconds(d.RetryAfter)))
		what := "requests per minute"
		if d.Reason == "tpm" {
			what = "tokens per minute"
		}
		writeError(w, http.StatusTooManyRequests, "rate_limit_error",
			"rate limit reached ("+what+"), retry in "+w.Header().Get("Retry-After")+" seconds")
		return nil, false
	}

	if d.Degraded {
		return nil, true // Redis is down, so there is nothing to correct later
	}
	return &reservation{
		h: h, tenantID: tenant.ID, limits: lim,
		estimate: estimate, promptEstimate: estimatePromptTokens(req), ctx: r.Context(),
	}, true
}

// setRateLimitHeaders tells the client how much is left (same names as OpenAI uses).
func setRateLimitHeaders(w http.ResponseWriter, lim ratelimit.Limits, d ratelimit.Decision) {
	hd := w.Header()
	hd.Set("X-RateLimit-Limit-Requests", strconv.Itoa(lim.RPM))
	hd.Set("X-RateLimit-Remaining-Requests", strconv.FormatInt(d.RemainingRequests, 10))
	hd.Set("X-RateLimit-Limit-Tokens", strconv.Itoa(lim.TPM))
	hd.Set("X-RateLimit-Remaining-Tokens", strconv.FormatInt(d.RemainingTokens, 10))
}

// adjust sends the correction to Redis. It must also work when the client has already left,
// so we detach from the request context (but keep a short timeout).
func (rv *reservation) adjust(delta int64) {
	if delta == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(rv.ctx), 2*time.Second)
	defer cancel()
	if err := rv.h.Limiter.Adjust(ctx, rv.tenantID, rv.limits, delta); err != nil {
		// Not fatal: the next minute the bucket heals itself.
		slog.Warn("could not correct token count", "request_id", RequestIDFrom(rv.ctx), "tenant_id", rv.tenantID, "err", err)
	}
}

// settle replaces our estimate with the real usage.
func (rv *reservation) settle(usage *provider.Usage, answerChars int) {
	if rv == nil {
		return
	}
	rv.adjust(actualTokens(usage, rv.promptEstimate, answerChars) - rv.estimate)
}

// refund gives all reserved tokens back (the provider failed, so nothing was used).
func (rv *reservation) refund() {
	if rv == nil {
		return
	}
	rv.adjust(-rv.estimate)
}
