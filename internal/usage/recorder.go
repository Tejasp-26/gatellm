package usage

import (
	"context"
	"log/slog"
	"time"
)

// Recorder is what the request handlers call. It must be fast and it must never fail the request.
//
//	normal path:  put the event in the Redis Stream (the consumer writes it to Postgres later)
//	Redis is down: write it to Postgres directly (slower, but nothing is lost)
//	both are down: log the event as one error line, so it can still be recovered from the logs
type Recorder struct {
	stream   *Stream
	fallback EventWriter
	timeout  time.Duration
}

func NewRecorder(stream *Stream, fallback EventWriter, timeout time.Duration) *Recorder {
	return &Recorder{stream: stream, fallback: fallback, timeout: timeout}
}

func (r *Recorder) Record(ctx context.Context, e Event) {
	// The client may already be gone, but the event must still be saved.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.timeout)
	defer cancel()

	err := r.stream.Publish(ctx, e)
	if err == nil {
		return
	}
	slog.Warn("could not publish usage event, writing it to Postgres directly",
		"request_id", e.RequestID, "err", err)

	// The first timeout may be used up, so the fallback gets its own.
	ctx2, cancel2 := context.WithTimeout(context.WithoutCancel(ctx), 3*r.timeout)
	defer cancel2()
	if err := r.fallback.Write(ctx2, []Event{e}); err != nil {
		slog.Error("USAGE EVENT LOST (stream and database both failed)",
			"request_id", e.RequestID, "tenant_id", e.TenantID, "provider", e.Provider, "model", e.Model,
			"prompt_tokens", e.PromptTokens, "completion_tokens", e.CompletionTokens,
			"cost_usd", e.CostUSD, "cache_status", e.CacheStatus, "status", e.Status,
			"created_at", e.CreatedAt, "err", err)
	}
}
