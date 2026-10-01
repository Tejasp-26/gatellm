package router

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"gatellm/internal/breaker"
	"gatellm/internal/provider"
)

// AutoModel is the model name a client sends to let the router choose.
const AutoModel = "auto"

// Served tells who answered, and who was tried before.
type Served struct {
	Provider string   // e.g. "groq"
	Model    string   // the real model name used at that provider
	Tried    []string // provider names in the order they were tried, e.g. ["groq", "gemini"]
}

// Router chooses a target and falls back to the next one when it fails.
type Router struct {
	targets  []*Target
	strategy Strategy
}

func New(targets []*Target, strategy Strategy) *Router {
	return &Router{targets: targets, strategy: strategy}
}

// order = targets that are up (in the order of the strategy), then the ones that are down.
// We still try the down ones at the end: if everything is down it is better to try than to give up,
// and a breaker that is open answers in a few microseconds anyway.
func (r *Router) order() []*Target {
	var up, down []*Target
	for _, t := range r.targets {
		if t.isUp() {
			up = append(up, t)
		} else {
			down = append(down, t)
		}
	}
	return append(r.strategy.Order(up), down...)
}

// shouldFallback says if another provider could do better.
//
//	no:  the client left, or the request itself is wrong (400, 413, 422): every provider would refuse it
//	yes: everything else: provider down, timeout, 429, breaker open, even 401/404 (a wrong key or
//	     model name at ONE provider should not stop the others from answering)
func shouldFallback(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	var pe *provider.ProviderError
	if errors.As(err, &pe) {
		switch pe.StatusCode {
		case 400, 413, 422:
			return false
		}
	}
	return true
}

func (t *Target) served(tried []string) Served {
	return Served{Provider: t.Name(), Model: t.Model, Tried: append([]string(nil), tried...)}
}

// Chat tries the targets one by one until one answers.
func (r *Router) Chat(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, Served, error) {
	var tried []string
	var lastErr error
	var last Served
	for _, t := range r.order() {
		tried = append(tried, t.Name())
		call := *req // a copy, so every target gets its own model name
		call.Model = t.Model

		start := time.Now()
		resp, err := t.Provider.Chat(ctx, &call)
		if err == nil {
			t.observe(time.Since(start))
			return resp, t.served(tried), nil
		}
		lastErr, last = err, t.served(tried)
		if !shouldFallback(err) || ctx.Err() != nil {
			return nil, last, err
		}
		slog.Warn("falling back to the next provider", "failed_provider", t.Name(), "err", err)
	}
	return nil, last, lastErr
}

// ChatStream is the same for streams. We can only fall back until the stream has STARTED.
// After the first byte went to the client, a failure is reported as it is.
func (r *Router) ChatStream(ctx context.Context, req *provider.ChatRequest) (<-chan provider.StreamChunk, Served, error) {
	var tried []string
	var lastErr error
	var last Served
	for _, t := range r.order() {
		tried = append(tried, t.Name())
		call := *req
		call.Model = t.Model

		start := time.Now()
		ch, err := t.Provider.ChatStream(ctx, &call)
		if err == nil {
			t.observe(time.Since(start)) // for streams: the time until the stream started
			return ch, t.served(tried), nil
		}
		lastErr, last = err, t.served(tried)
		if !shouldFallback(err) || ctx.Err() != nil {
			return nil, last, err
		}
		slog.Warn("falling back to the next provider", "failed_provider", t.Name(), "err", err)
	}
	return nil, last, lastErr
}

// TargetStatus is a snapshot for the stats page (Phase 8) and for tests.
type TargetStatus struct {
	Provider     string    `json:"provider"`
	Model        string    `json:"model"`
	Weight       int       `json:"weight"`
	Breaker      string    `json:"breaker"` // closed, open, half-open, or "n/a"
	AvgLatencyMs float64   `json:"avg_latency_ms"`
	LastCheck    time.Time `json:"last_check"`
	CheckOK      bool      `json:"check_ok"`
	CheckError   string    `json:"check_error,omitempty"`
}

// Status returns the health of every target.
func (r *Router) Status() []TargetStatus {
	out := make([]TargetStatus, 0, len(r.targets))
	for _, t := range r.targets {
		state := "n/a"
		if b, ok := t.Provider.(interface{ BreakerState() breaker.State }); ok {
			state = b.BreakerState().String()
		}
		t.mu.Lock()
		out = append(out, TargetStatus{
			Provider: t.Name(), Model: t.Model, Weight: t.Weight, Breaker: state,
			AvgLatencyMs: t.avgLatency, LastCheck: t.lastCheck, CheckOK: t.checkOK, CheckError: t.checkErr,
		})
		t.mu.Unlock()
	}
	return out
}
