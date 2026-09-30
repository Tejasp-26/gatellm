package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"gatellm/internal/provider"
)

// These structs describe one streamed chunk in the OpenAI format.
type sseDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

type sseChoice struct {
	Index        int      `json:"index"`
	Delta        sseDelta `json:"delta"`
	FinishReason *string  `json:"finish_reason"` // null until the last text chunk
}

type sseChunk struct {
	ID      string          `json:"id"`
	Object  string          `json:"object"`
	Created int64           `json:"created"`
	Model   string          `json:"model"`
	Choices []sseChoice     `json:"choices"`
	Usage   *provider.Usage `json:"usage,omitempty"`
}

// writeSSE writes one event ("data: ...") and sends it to the client immediately.
// It returns an error when the client is gone.
func writeSSE(w http.ResponseWriter, f http.Flusher, payload []byte) error {
	if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
		return err
	}
	f.Flush() // without this, Go would keep the data in a buffer
	return nil
}

func writeSSEJSON(w http.ResponseWriter, f http.Flusher, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeSSE(w, f, payload)
}

// streamChat answers a chat request with a Server-Sent Events stream.
func (h *Handler) streamChat(w http.ResponseWriter, r *http.Request, p provider.Provider, req *provider.ChatRequest) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "server_error", "streaming is not supported here")
		return
	}
	includeUsage := req.StreamOptions != nil && req.StreamOptions.IncludeUsage

	// When this function returns, cancel() stops the provider call.
	// The same happens if the client disconnects, because r.Context() is cancelled then.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	start := time.Now()
	chunks, err := p.ChatStream(ctx, req)
	if err != nil {
		// We have not sent anything yet, so a normal JSON error is still possible.
		slog.Warn("provider stream failed to start",
			"request_id", RequestIDFrom(ctx),
			"tenant_id", tenantIDFrom(ctx),
			"provider", p.Name(),
			"err", err,
		)
		writeError(w, http.StatusBadGateway, "upstream_error", "the "+p.Name()+" provider failed, please try again")
		return
	}

	// From here on the status is 200 and can no longer change.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no") // tells proxies like nginx not to hold the data back
	w.Header().Set("X-Provider", p.Name())
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	id := "chatcmpl-" + RequestIDFrom(ctx)
	created := time.Now().Unix()
	newChunk := func(delta sseDelta, finish *string) sseChunk {
		return sseChunk{
			ID: id, Object: "chat.completion.chunk", Created: created, Model: req.Model,
			Choices: []sseChoice{{Index: 0, Delta: delta, FinishReason: finish}},
		}
	}

	// The first chunk only announces the role, like OpenAI does.
	if err := writeSSEJSON(w, flusher, newChunk(sseDelta{Role: "assistant"}, nil)); err != nil {
		return
	}

	var usage *provider.Usage
	pieces := 0
	for chunk := range chunks {
		if chunk.Err != nil {
			// The provider broke in the middle. We cannot change the 200 status any more,
			// so we send an error event and end the stream (no [DONE]).
			slog.Warn("provider stream failed in the middle",
				"request_id", RequestIDFrom(ctx),
				"tenant_id", tenantIDFrom(ctx),
				"provider", p.Name(),
				"pieces_sent", pieces,
				"err", chunk.Err,
			)
			writeSSEJSON(w, flusher, map[string]any{
				"error": map[string]string{
					"message": "the " + p.Name() + " provider failed during the stream",
					"type":    "upstream_error",
				},
			})
			return
		}
		if chunk.Usage != nil {
			usage = chunk.Usage
		}
		if chunk.Content == "" && chunk.FinishReason == "" {
			continue // a usage-only chunk, nothing to show
		}

		var finish *string
		if chunk.FinishReason != "" {
			reason := chunk.FinishReason
			finish = &reason
		}
		if err := writeSSEJSON(w, flusher, newChunk(sseDelta{Content: chunk.Content}, finish)); err != nil {
			// Writing failed: the client is gone. The deferred cancel() stops the provider.
			slog.Info("client disconnected during stream",
				"request_id", RequestIDFrom(ctx), "provider", p.Name(), "pieces_sent", pieces)
			return
		}
		pieces++
	}

	// The channel closes on its own when the answer is complete, or when ctx was cancelled.
	if ctx.Err() != nil {
		slog.Info("client disconnected during stream",
			"request_id", RequestIDFrom(ctx), "provider", p.Name(), "pieces_sent", pieces)
		return
	}

	// Only if the client asked for it, send the token usage as a last chunk with no choices.
	if includeUsage && usage != nil {
		writeSSEJSON(w, flusher, sseChunk{
			ID: id, Object: "chat.completion.chunk", Created: created, Model: req.Model,
			Choices: []sseChoice{}, Usage: usage,
		})
	}
	writeSSE(w, flusher, []byte("[DONE]"))

	// One summary line per stream. Never the text itself.
	attrs := []any{
		"request_id", RequestIDFrom(ctx),
		"tenant_id", tenantIDFrom(ctx),
		"provider", p.Name(),
		"pieces", pieces,
		"duration_ms", time.Since(start).Milliseconds(),
	}
	if usage != nil {
		attrs = append(attrs, "prompt_tokens", usage.PromptTokens, "completion_tokens", usage.CompletionTokens)
	}
	slog.Info("stream finished", attrs...)
}
