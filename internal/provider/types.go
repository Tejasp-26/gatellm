// Package provider defines what an LLM provider looks like to the gateway
// and contains the concrete providers (mock, groq, gemini).
package provider

import (
	"errors"
	"fmt"
)

// The types below follow the OpenAI chat format, so any OpenAI client works with our gateway.

// Message is one chat message. Role is "system", "user" or "assistant".
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatRequest is what the client sends to POST /v1/chat/completions.
type ChatRequest struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Temperature *float64  `json:"temperature,omitempty"` // pointer, so 0 and "not set" are different
	MaxTokens   int       `json:"max_tokens,omitempty"`
	Stream      bool      `json:"stream,omitempty"`
}

// Choice is one answer from the model.
type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

// Usage is the token count. We need it later for rate limits and cost.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// ChatResponse is what we send back to the client.
type ChatResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   Usage    `json:"usage"`
}

// StreamChunk is one piece of a streaming answer (used from Phase 3).
type StreamChunk struct {
	Content      string
	FinishReason string
	Err          error
}

// ProviderError is returned when the provider answers with an HTTP error.
// We keep the status code, because Phase 5 retries only on 429 and 5xx.
type ProviderError struct {
	StatusCode int
	Message    string
}

func (e *ProviderError) Error() string {
	return fmt.Sprintf("provider returned status %d: %s", e.StatusCode, e.Message)
}

// ErrStreamingNotReady is returned by ChatStream until we build streaming in Phase 3.
var ErrStreamingNotReady = errors.New("streaming is not implemented yet")
