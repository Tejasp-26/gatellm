package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"unicode/utf8"

	"gatellm/internal/breaker"
	"gatellm/internal/cache"
	"gatellm/internal/provider"
	"gatellm/internal/router"
)

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

// writeUpstreamError tells the client that the provider call failed.
// 502: the provider failed.  503: we did not even try, because the circuit breaker is open.
// sv says who was tried. More than one name means the router tried several providers.
func writeUpstreamError(w http.ResponseWriter, sv router.Served, err error) {
	several := len(sv.Tried) > 1
	tried := strings.Join(sv.Tried, ", ")

	if errors.Is(err, breaker.ErrOpen) {
		msg := "the " + sv.Provider + " provider is having problems and is paused for a short time, please try again shortly"
		if several {
			msg = "no provider is available right now (tried: " + tried + "), please try again shortly"
		}
		writeError(w, http.StatusServiceUnavailable, "upstream_unavailable", msg)
		return
	}
	msg := "the " + sv.Provider + " provider failed, please try again"
	if several {
		msg = "all providers failed (tried: " + tried + "), please try again"
	}
	writeError(w, http.StatusBadGateway, "upstream_error", msg)
}

// backend is whatever answers the request: one provider that the client picked,
// or the router (model "auto"). Both give back who really answered.
type backend struct {
	auto   bool
	chat   func(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, router.Served, error)
	stream func(ctx context.Context, req *provider.ChatRequest) (<-chan provider.StreamChunk, router.Served, error)
}

// pinned is the backend for a client that named the provider, like "groq/llama-3.1-8b-instant".
// There is no fallback: the client asked for exactly this one.
func pinned(p provider.Provider, model string) backend {
	served := router.Served{Provider: p.Name(), Model: model, Tried: []string{p.Name()}}
	return backend{
		chat: func(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, router.Served, error) {
			resp, err := p.Chat(ctx, req)
			return resp, served, err
		},
		stream: func(ctx context.Context, req *provider.ChatRequest) (<-chan provider.StreamChunk, router.Served, error) {
			ch, err := p.ChatStream(ctx, req)
			return ch, served, err
		},
	}
}

// setRouteHeader shows, for model "auto", which providers were tried (in order).
func setRouteHeader(w http.ResponseWriter, be backend, sv router.Served) {
	if be.auto && len(sv.Tried) > 0 {
		w.Header().Set("X-Route-Tried", strings.Join(sv.Tried, ","))
	}
}

// ChatCompletions handles POST /v1/chat/completions (OpenAI format).
// The Auth middleware runs before it, so the tenant is already known.
// The client picks the provider inside the model name:
//
//	"mock"                          -> mock provider
//	"groq/llama-3.1-8b-instant"     -> groq, model llama-3.1-8b-instant (no fallback)
//	"gemini/gemini-2.5-flash"       -> gemini, model gemini-2.5-flash (no fallback)
//	"auto"                          -> the router chooses, and falls back if a provider fails
func (h *Handler) ChatCompletions(w http.ResponseWriter, r *http.Request) {
	// 1. Read the JSON body.
	var req provider.ChatRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	clientModel := req.Model // the model exactly as the client sent it, used for the cache key

	// 2. Check the fields.
	if msg := validate(&req); msg != "" {
		writeError(w, http.StatusBadRequest, "invalid_request_error", msg)
		return
	}

	// 3. Find who will answer: the router ("auto") or the provider named in the model.
	var be backend
	if req.Model == router.AutoModel {
		if h.Router == nil {
			writeError(w, http.StatusBadRequest, "invalid_request_error", `the model "auto" is not available on this gateway`)
			return
		}
		be = backend{auto: true, chat: h.Router.Chat, stream: h.Router.ChatStream}
	} else {
		providerName, modelName, _ := strings.Cut(req.Model, "/")
		p, ok := h.Providers[providerName]
		if !ok {
			writeError(w, http.StatusBadRequest, "invalid_request_error",
				fmt.Sprintf("unknown provider %q, available: %s, or use the model \"auto\"", providerName, strings.Join(h.Providers.Names(), ", ")))
			return
		}
		if modelName == "" && !strings.HasPrefix(providerName, "mock") {
			writeError(w, http.StatusBadRequest, "invalid_request_error",
				fmt.Sprintf("use the format %s/<model-name>", providerName))
			return
		}
		req.Model = modelName
		be = pinned(p, modelName)
	}

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

	// Cache: only for non-streaming requests with temperature 0 (see cache.Cacheable).
	// Other requests get "X-Cache: BYPASS". If the cache is switched off there is no header.
	if h.Cache != nil {
		if cache.Cacheable(&req, h.CacheAllowTemperature) {
			h.chatCached(w, r, be, &req, clientModel, res)
			return
		}
		w.Header().Set("X-Cache", "BYPASS")
	}

	// Streaming has its own flow.
	if req.Stream {
		h.streamChat(w, r, be, &req, res)
		return
	}

	// 6. Call the provider (or the router, which may try several).
	resp, sv, err := be.chat(r.Context(), &req)
	setRouteHeader(w, be, sv)
	h.noteFallback(sv, err)
	if err != nil {
		res.refund() // nothing was used, give the reserved tokens back
		h.recordUsage(r.Context(), usageInfo{provider: sv.Provider, model: sv.Model,
			cacheStatus: h.plainCacheStatus(), status: endStatus(r.Context())})
		// The full error goes to the log. The client gets a short, safe message.
		slog.Warn("provider call failed",
			"request_id", RequestIDFrom(r.Context()),
			"tenant_id", tenantIDFrom(r.Context()),
			"provider", sv.Provider,
			"tried", sv.Tried,
			"err", err,
		)
		writeUpstreamError(w, sv, err)
		return
	}

	// 7. Replace the estimate with the real token count, add the cost to the monthly
	// budget, then send the answer.
	answerChars := 0
	if len(resp.Choices) > 0 {
		answerChars = utf8.RuneCountInString(resp.Choices[0].Message.Content)
	}
	res.settle(&resp.Usage, answerChars)
	h.recordSpend(r.Context(), sv.Provider, sv.Model, &resp.Usage, estimatePromptTokens(&req), answerChars)
	h.recordUsage(r.Context(), usageInfo{provider: sv.Provider, model: sv.Model, cacheStatus: h.plainCacheStatus(),
		status: "ok", charged: true, u: &resp.Usage, promptEstimate: estimatePromptTokens(&req), answerChars: answerChars})
	w.Header().Set("X-Provider", sv.Provider)
	writeJSON(w, http.StatusOK, resp)
}
