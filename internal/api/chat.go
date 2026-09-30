package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"gatellm/internal/provider"
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

// ChatCompletions handles POST /v1/chat/completions (OpenAI format).
// The client picks the provider inside the model name:
//
//	"mock"                          -> mock provider
//	"groq/llama-3.1-8b-instant"     -> groq, model llama-3.1-8b-instant
//	"gemini/gemini-2.5-flash"       -> gemini, model gemini-2.5-flash
func (h *Handler) ChatCompletions(w http.ResponseWriter, r *http.Request) {
	// 1. Read the JSON body.
	var req provider.ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "invalid_request_error", "request body is too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_request_error", "body is not valid JSON")
		return
	}

	// 2. Check the fields.
	if msg := validate(&req); msg != "" {
		writeError(w, http.StatusBadRequest, "invalid_request_error", msg)
		return
	}
	if req.Stream {
		writeError(w, http.StatusNotImplemented, "invalid_request_error", "streaming is not available yet")
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

	// 4. Call the provider.
	resp, err := p.Chat(r.Context(), &req)
	if err != nil {
		// The full error goes to the log. The client gets a short, safe message.
		slog.Warn("provider call failed",
			"request_id", RequestIDFrom(r.Context()),
			"provider", p.Name(),
			"err", err,
		)
		writeError(w, http.StatusBadGateway, "upstream_error", "the "+p.Name()+" provider failed, please try again")
		return
	}

	// 5. Send the answer.
	w.Header().Set("X-Provider", p.Name())
	writeJSON(w, http.StatusOK, resp)
}
