package provider

import (
	"context"
	"errors"
	"runtime"
	"strings"
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

// collect reads a stream until the channel closes. It fails the test if that takes too long.
func collect(t *testing.T, ch <-chan StreamChunk) []StreamChunk {
	t.Helper()
	var chunks []StreamChunk
	timeout := time.After(3 * time.Second)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				return chunks
			}
			chunks = append(chunks, c)
		case <-timeout:
			t.Fatal("stream did not close in time")
		}
	}
}

func TestMockStreamWholeAnswer(t *testing.T) {
	m := NewMock(0, 0)
	req := &ChatRequest{Messages: []Message{{Role: "user", Content: "hello there"}}}

	ch, err := m.ChatStream(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)

	var text strings.Builder
	for _, c := range chunks {
		if c.Err != nil {
			t.Fatalf("unexpected error chunk: %v", c.Err)
		}
		text.WriteString(c.Content)
	}
	if text.String() != "Mock reply to: hello there" {
		t.Errorf("wrong streamed text: %q", text.String())
	}
	last := chunks[len(chunks)-1]
	if last.FinishReason != "stop" || last.Usage == nil || last.Usage.TotalTokens == 0 {
		t.Errorf("last chunk should have finish reason and usage: %+v", last)
	}
}

func TestMockStreamFailsInTheMiddle(t *testing.T) {
	m := NewMock(0, 0)
	req := &ChatRequest{Messages: []Message{{Role: "user", Content: "[fail-midstream] please"}}}

	ch, err := m.ChatStream(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)

	// Two words, then the error, then nothing more.
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d: %+v", len(chunks), chunks)
	}
	if chunks[0].Content == "" || chunks[1].Content == "" || chunks[2].Err == nil {
		t.Errorf("unexpected chunks: %+v", chunks)
	}
}

func TestMockStreamFailsAtStart(t *testing.T) {
	m := NewMock(0, 1)
	ch, err := m.ChatStream(context.Background(), &ChatRequest{Messages: []Message{{Role: "user", Content: "hi"}}})

	var pe *ProviderError
	if !errors.As(err, &pe) || ch != nil {
		t.Errorf("expected ProviderError and no channel, got %v, %v", err, ch)
	}
}

func TestMockStreamStopsWhenCancelled(t *testing.T) {
	m := NewMock(0, 0)
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := m.ChatStream(ctx, &ChatRequest{Messages: []Message{{Role: "user", Content: "a b c d e f g h"}}})
	if err != nil {
		t.Fatal(err)
	}

	<-ch     // read one piece
	cancel() // the client leaves

	// The channel must close soon.
	collect(t, ch)
}

// If nobody reads the channel any more (the client left), the goroutine must still end.
// A goroutine that waits forever on a full channel is a "goroutine leak".
func TestMockStreamDoesNotLeakGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()

	m := NewMock(0, 0)
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := m.ChatStream(ctx, &ChatRequest{Messages: []Message{{Role: "user", Content: "a b c d e f g h"}}})
	if err != nil {
		t.Fatal(err)
	}
	<-ch // read one piece, then stop reading

	// Wait longer than the pause between words, so the goroutine is now
	// stuck trying to send the next word to a reader that is gone.
	time.Sleep(4 * mockWordDelay)
	cancel() // the client leaves

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: %d goroutines before, %d after", before, runtime.NumGoroutine())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
