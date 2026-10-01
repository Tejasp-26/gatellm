package api

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"unicode/utf8"

	"gatellm/internal/breaker"
	"gatellm/internal/provider"
)

// writeUpstreamError tells the client that the provider call failed.
// 502: the provider failed.  503: we did not even try, because its circuit breaker is open.
func writeUpstreamError(w http.ResponseWriter, p provider.Provider, err error) {
	if errors.Is(err, breaker.ErrOpen) {
		writeError(w, http.StatusServiceUnavailable, "upstream_unavailable",
			"the "+p.Name()+" provider is having problems and is paused for a short time, please try again shortly")
		return
	}
	writeError(w, http.StatusBadGateway, "upstream_error", "the "+p.Name()+" provider failed, please try again")
}

// validate checks the request and returns an error message ("" means OK).
func validate(req *provider.ChatRequest) string {
	if req.Model == "" {
		return "model is required"
	}
	if len(req.Messages) == 0 {
		return "messages must not be empty"
	}
	for i, m := range req.Messages {
		if m.Role != "system" && m.Role != "user" && m.Role != "assistant" {
			return fmt.Sprintf("messages[%d].role must be system, user or assistant", i)
		}
		if m.Content == "" {
			return fmt.Sprintf("messages[%d].content must not be empty", i)
		}
	}
	return ""
}

// ChatCompletions handles POST /v1/chat/completions (OpenAI format).
// The Auth middleware runs before it, so the tenant is already known.
// The client picks the provider inside the model name:
//
//	"mock"                          -> mock provider
//	"groq/llama-3.1-8b-instant"     -> groq, model llama-3.1-8b-instant
//	"gemini/gemini-2.5-flash"       -> gemini, model gemini-2.5-flash
func (h *Handler) ChatCompletions(w http.ResponseWriter, r *http.Request) {
	// 1. Read the JSON body.
	var req provider.ChatRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	// 2. Check the fields.
	if msg := validate(&req); msg != "" {
		writeError(w, http.StatusBadRequest, "invalid_request_error", msg)
		return
	}
	// 3. Find the provider from the model name.
	providerName, modelName, _ := strings.Cut(req.Model, "/")
	p, ok := h.Providers[providerName]
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_request_error",
			fmt.Sprintf("unknown provider %q, available: %s", providerName, strings.Join(h.Providers.Names(), ", ")))
		return
	}
	if modelName == "" && providerName != "mock" {
		writeError(w, http.StatusBadRequest, "invalid_request_error",
			fmt.Sprintf("use the format %s/<model-name>", providerName))
		return
	}
	req.Model = modelName

	// 4. Check that the monthly budget is not used up. Do this first, so a rejected
	// request does not use up any rate limit.
	if !h.checkBudget(w, r) {
		return
	}

	// 5. Check the rate limits. This takes a request and an estimate of tokens from the tenant.
	res, ok := h.reserve(w, r, &req)
	if !ok {
		return
	}

	// Streaming has its own flow.
	if req.Stream {
		h.streamChat(w, r, p, &req, res)
		return
	}

	// 6. Call the provider.
	resp, err := p.Chat(r.Context(), &req)
	if err != nil {
		res.refund() // nothing was used, give the reserved tokens back
		// The full error goes to the log. The client gets a short, safe message.
		slog.Warn("provider call failed",
			"request_id", RequestIDFrom(r.Context()),
			"tenant_id", tenantIDFrom(r.Context()),
			"provider", p.Name(),
			"err", err,
		)
		writeUpstreamError(w, p, err)
		return
	}

	// 7. Replace the estimate with the real token count, add the cost to the monthly
	// budget, then send the answer.
	answerChars := 0
	if len(resp.Choices) > 0 {
		answerChars = utf8.RuneCountInString(resp.Choices[0].Message.Content)
	}
	res.settle(&resp.Usage, answerChars)
	h.recordSpend(r.Context(), p.Name(), req.Model, &resp.Usage, estimatePromptTokens(&req), answerChars)
	w.Header().Set("X-Provider", p.Name())
	writeJSON(w, http.StatusOK, resp)
}
