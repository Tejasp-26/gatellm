package provider

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// Mock is a fake provider. It costs nothing and lets us test slow or failing providers.
// We use it for development, load tests and failure tests.
type Mock struct {
	latency   time.Duration // how long each call takes
	errorRate float64       // 0 = never fails, 1 = always fails, 0.3 = fails 30% of the time
}

func NewMock(latency time.Duration, errorRate float64) *Mock {
	return &Mock{latency: latency, errorRate: errorRate}
}

func (m *Mock) Name() string { return "mock" }

func (m *Mock) Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	// Wait for the fake latency, but stop early if the caller cancels.
	select {
	case <-time.After(m.latency):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Fail randomly, like a real provider having a bad day.
	if rand.Float64() < m.errorRate {
		return nil, &ProviderError{StatusCode: 503, Message: "mock provider simulated failure"}
	}

	// Reply by echoing the last user message.
	lastUser := ""
	for _, msg := range req.Messages {
		if msg.Role == "user" {
			lastUser = msg.Content
		}
	}
	answer := "Mock reply to: " + lastUser

	// Token counts are estimated: about 1 token per 4 characters.
	promptTokens := 0
	for _, msg := range req.Messages {
		promptTokens += len(msg.Content) / 4
	}
	completionTokens := len(answer) / 4

	return &ChatResponse{
		ID:      fmt.Sprintf("chatcmpl-mock-%d", time.Now().UnixNano()),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   "mock",
		Choices: []Choice{{
			Index:        0,
			Message:      Message{Role: "assistant", Content: answer},
			FinishReason: "stop",
		}},
		Usage: Usage{
			PromptTokens:     promptTokens,
			CompletionTokens: completionTokens,
			TotalTokens:      promptTokens + completionTokens,
		},
	}, nil
}

func (m *Mock) ChatStream(ctx context.Context, req *ChatRequest) (<-chan StreamChunk, error) {
	return nil, ErrStreamingNotReady
}
