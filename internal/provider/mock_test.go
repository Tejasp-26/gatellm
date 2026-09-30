package provider

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMockReply(t *testing.T) {
	m := NewMock(0, 0) // no delay, never fails
	req := &ChatRequest{Messages: []Message{{Role: "user", Content: "hello"}}}

	resp, err := m.Chat(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := resp.Choices[0].Message.Content
	if got != "Mock reply to: hello" {
		t.Errorf("wrong reply: %q", got)
	}
	if resp.Usage.TotalTokens == 0 {
		t.Error("usage should not be zero")
	}
}

func TestMockAlwaysFails(t *testing.T) {
	m := NewMock(0, 1) // error rate 100%
	req := &ChatRequest{Messages: []Message{{Role: "user", Content: "hi"}}}

	_, err := m.Chat(context.Background(), req)
	var pe *ProviderError
	if !errors.As(err, &pe) || pe.StatusCode != 503 {
		t.Errorf("expected ProviderError 503, got %v", err)
	}
}

func TestMockStopsWhenCancelled(t *testing.T) {
	m := NewMock(2*time.Second, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := m.Chat(ctx, &ChatRequest{})
	if err == nil {
		t.Fatal("expected an error")
	}
	if time.Since(start) > time.Second {
		t.Error("mock did not stop early after cancel")
	}
}
