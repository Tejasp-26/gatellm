package provider

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

const (
	// Pause between two streamed words.
	mockWordDelay = 30 * time.Millisecond

	// If the last user message contains this text, the mock stream breaks after two words.
	// It lets us test "provider fails in the middle of a stream" on demand.
	failMidStreamTrigger = "[fail-midstream]"
)

// Mock is a fake provider. It costs nothing and lets us test slow or failing providers.
// We use it for development, load tests and failure tests.
type Mock struct {
	latency   time.Duration // wait before the answer (or before the first word when streaming)
	errorRate float64       // 0 = never fails, 1 = always fails, 0.3 = fails 30% of the time
}

func NewMock(latency time.Duration, errorRate float64) *Mock {
	return &Mock{latency: latency, errorRate: errorRate}
}

func (m *Mock) Name() string { return "mock" }

// lastUserMessage returns the text of the last "user" message.
func lastUserMessage(req *ChatRequest) string {
	last := ""
	for _, msg := range req.Messages {
		if msg.Role == "user" {
			last = msg.Content
		}
	}
	return last
}

// mockAnswer builds the fake answer (an echo) and its token counts.
// Token counts are estimated: about 1 token per 4 characters.
func mockAnswer(req *ChatRequest) (string, Usage) {
	answer := "Mock reply to: " + lastUserMessage(req)

	promptTokens := 0
	for _, msg := range req.Messages {
		promptTokens += len(msg.Content) / 4
	}
	completionTokens := len(answer) / 4

	return answer, Usage{
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		TotalTokens:      promptTokens + completionTokens,
	}
}

func (m *Mock) Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	if !sleepCtx(ctx, m.latency) {
		return nil, ctx.Err()
	}

	// Fail randomly, like a real provider having a bad day.
	if rand.Float64() < m.errorRate {
		return nil, &ProviderError{StatusCode: 503, Message: "mock provider simulated failure"}
	}

	answer, usage := mockAnswer(req)
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
		Usage: usage,
	}, nil
}

// ChatStream sends the same fake answer word by word.
func (m *Mock) ChatStream(ctx context.Context, req *ChatRequest) (<-chan StreamChunk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A failure at the start behaves like a provider that answers with an error status.
	if rand.Float64() < m.errorRate {
		return nil, &ProviderError{StatusCode: 503, Message: "mock provider simulated failure"}
	}

	answer, usage := mockAnswer(req)
	words := strings.Fields(answer)
	failInMiddle := strings.Contains(lastUserMessage(req), failMidStreamTrigger)

	ch := make(chan StreamChunk)
	go func() {
		defer close(ch) // the channel is always closed when this goroutine ends

		if !sleepCtx(ctx, m.latency) {
			return
		}
		for i, word := range words {
			if failInMiddle && i == 2 {
				sendChunk(ctx, ch, StreamChunk{Err: errors.New("mock provider simulated failure in the middle of the stream")})
				return
			}
			text := word
			if i > 0 {
				text = " " + word // put the spaces back
			}
			if !sendChunk(ctx, ch, StreamChunk{Content: text}) {
				return
			}
			if !sleepCtx(ctx, mockWordDelay) {
				return
			}
		}
		sendChunk(ctx, ch, StreamChunk{FinishReason: "stop", Usage: &usage})
	}()
	return ch, nil
}
