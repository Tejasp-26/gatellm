package provider

import (
	"context"
	"sort"
)

// Provider is the one interface every LLM provider must follow.
// The rest of the gateway only knows this interface, never Groq or Gemini directly.
type Provider interface {
	// Name is used in logs, headers and metrics, e.g. "groq".
	Name() string
	// Chat sends the request and waits for the full answer.
	Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error)
	// ChatStream sends the request and returns the answer piece by piece (Phase 3).
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
