package provider

import (
	"context"
	"sort"
	"time"
)

// Provider is the one interface every LLM provider must follow.
// The rest of the gateway only knows this interface, never Groq or Gemini directly.
type Provider interface {
	// Name is used in logs, headers and metrics, e.g. "groq".
	Name() string
	// Chat sends the request and waits for the full answer.
	Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error)
	// ChatStream sends the request and returns the answer piece by piece.
	//
	// Rules of the stream:
	//   - If the provider cannot be reached or answers with an error status, ChatStream
	//     returns an error and no channel. Nothing has been sent to the client yet, so
	//     the gateway can still return a clean error (or try another provider).
	//   - Otherwise the channel delivers chunks and is closed when the answer is complete.
	//   - If something fails in the middle, one chunk with Err is sent, then the channel closes.
	//   - When ctx is cancelled (for example the client left), the provider call is
	//     stopped and the channel is closed.
	ChatStream(ctx context.Context, req *ChatRequest) (<-chan StreamChunk, error)
}

// Registry holds all enabled providers, found by name.
type Registry map[string]Provider

// Names returns the enabled provider names in sorted order (used in error messages).
func (r Registry) Names() []string {
	names := make([]string, 0, len(r))
	for name := range r {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// sendChunk puts a chunk on the channel, but gives up if ctx is cancelled.
// Without this, a goroutine could wait forever for a client that already left.
// It returns false when the caller should stop.
func sendChunk(ctx context.Context, ch chan<- StreamChunk, c StreamChunk) bool {
	select {
	case ch <- c:
		return true
	case <-ctx.Done():
		return false
	}
}

// sleepCtx waits for d, but stops early if ctx is cancelled. It returns false if cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
