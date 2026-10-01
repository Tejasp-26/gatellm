package api

import (
	"context"
	"time"

	"gatellm/internal/provider"
	"gatellm/internal/usage"
)

// UsageRecorder is what the handlers need to record usage.
// The real one is usage.Recorder (Redis Stream, with a fallback to Postgres). Tests use a fake.
// Record must be fast and must never fail the request.
type UsageRecorder interface {
	Record(ctx context.Context, e usage.Event)
}

// usageInfo describes how one request ended.
type usageInfo struct {
	provider, model string
	cacheStatus     string // MISS, HIT-EXACT, HIT-SEMANTIC, COALESCED, BYPASS or OFF
	status          string // ok, error, cancelled or stream_error

	// charged is true when we really paid a provider for this request. Cache hits, coalesced
	// followers and failures cost nothing, so their tokens and cost stay 0.
	charged        bool
	u              *provider.Usage
	promptEstimate int64
	answerChars    int
}

// recordUsage builds the event and hands it to the recorder.
// ctx must be the request context: it carries the request id, the tenant and the start time.
func (h *Handler) recordUsage(ctx context.Context, in usageInfo) {
	tenant := TenantFrom(ctx)
	id := RequestIDFrom(ctx)
	if h.Usage == nil || tenant == nil || id == "" {
		return
	}
	model := in.model
	if model == "" {
		model = in.provider // the mock has no model name
	}
	ev := usage.Event{
		RequestID: id, TenantID: tenant.ID, Provider: in.provider, Model: model,
		LatencyMS: latencyMS(ctx), CacheStatus: in.cacheStatus, Status: in.status,
		CreatedAt: time.Now().UTC(),
	}
	if in.charged {
		ev.PromptTokens, ev.CompletionTokens, ev.CostUSD = chargeOf(in.provider, in.model, in.u, in.promptEstimate, in.answerChars)
	}
	h.Usage.Record(ctx, ev)
}

// plainCacheStatus is the cache status of a request that does not go through the cache code.
func (h *Handler) plainCacheStatus() string {
	if h.Cache != nil {
		return "BYPASS" // the cache is on, but this request may not use it (stream, temperature > 0)
	}
	return "OFF"
}

// endStatus is the status of a failed request: "cancelled" if the client left, otherwise "error".
func endStatus(ctx context.Context) string {
	if ctx.Err() != nil {
		return "cancelled"
	}
	return "error"
}
