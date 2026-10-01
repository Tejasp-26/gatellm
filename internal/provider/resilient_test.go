package provider

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gatellm/internal/breaker"
)

// fakeProvider does whatever the test tells it to. n is the number of the call (1, 2, 3 ...).
type fakeProvider struct {
	mu          sync.Mutex
	chatCalls   int
	streamCalls int
	chat        func(ctx context.Context, n int) (*ChatResponse, error)
	stream      func(ctx context.Context, n int) (<-chan StreamChunk, error)
}

func (f *fakeProvider) Name() string { return "fake" }

func (f *fakeProvider) Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	f.mu.Lock()
	f.chatCalls++
	n := f.chatCalls
	f.mu.Unlock()
	return f.chat(ctx, n)
}

func (f *fakeProvider) ChatStream(ctx context.Context, req *ChatRequest) (<-chan StreamChunk, error) {
	f.mu.Lock()
	f.streamCalls++
	n := f.streamCalls
	f.mu.Unlock()
	return f.stream(ctx, n)
}

func (f *fakeProvider) calls() (chat, stream int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.chatCalls, f.streamCalls
}

func okResponse() *ChatResponse {
	return &ChatResponse{Choices: []Choice{{Message: Message{Role: "assistant", Content: "ok"}}}}
}

func status(code int) error { return &ProviderError{StatusCode: code} }

// testCfg uses tiny waits so the tests are fast.
func testCfg(timeout time.Duration, attempts, threshold int, cooldown time.Duration) ResilientConfig {
	return ResilientConfig{
		Timeout: timeout,
		Retry:   RetryConfig{MaxAttempts: attempts, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond},
		Breaker: breaker.Config{FailureThreshold: threshold, Cooldown: cooldown},
	}
}

var anyRequest = &ChatRequest{Model: "x", Messages: []Message{{Role: "user", Content: "hi"}}}

// streamOf makes a finished stream with the given chunks.
func streamOf(chunks ...StreamChunk) <-chan StreamChunk {
	ch := make(chan StreamChunk, len(chunks))
	for _, c := range chunks {
		ch <- c
	}
	close(ch)
	return ch
}

func TestResilientNameIsTheInnerName(t *testing.T) {
	r := NewResilient(&fakeProvider{}, testCfg(time.Second, 1, 5, time.Second))
	if r.Name() != "fake" {
		t.Errorf("name = %q", r.Name())
	}
}

func TestChatRetriesAndThenWorks(t *testing.T) {
	fp := &fakeProvider{chat: func(ctx context.Context, n int) (*ChatResponse, error) {
		if n < 3 {
			return nil, status(503)
		}
		return okResponse(), nil
	}}
	r := NewResilient(fp, testCfg(time.Second, 3, 10, time.Second))

	resp, err := r.Chat(context.Background(), anyRequest)
	if err != nil || resp.Choices[0].Message.Content != "ok" {
		t.Fatalf("expected success after retries: %v", err)
	}
	if c, _ := fp.calls(); c != 3 {
		t.Errorf("provider calls = %d, want 3", c)
	}
}

func TestChatDoesNotRetryABadRequest(t *testing.T) {
	fp := &fakeProvider{chat: func(ctx context.Context, n int) (*ChatResponse, error) { return nil, status(400) }}
	r := NewResilient(fp, testCfg(time.Second, 3, 10, time.Second))

	_, err := r.Chat(context.Background(), anyRequest)
	var pe *ProviderError
	if !errors.As(err, &pe) || pe.StatusCode != 400 {
		t.Fatalf("expected the 400 back, got %v", err)
	}
	if c, _ := fp.calls(); c != 1 {
		t.Errorf("provider calls = %d, want 1 (no retry)", c)
	}
}

func TestChatTimeoutPerAttempt(t *testing.T) {
	// The provider never answers; it only stops when its context ends.
	fp := &fakeProvider{chat: func(ctx context.Context, n int) (*ChatResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	r := NewResilient(fp, testCfg(30*time.Millisecond, 2, 10, time.Second))

	start := time.Now()
	_, err := r.Chat(context.Background(), anyRequest)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected a timeout error, got %v", err)
	}
	if c, _ := fp.calls(); c != 2 {
		t.Errorf("a timeout is retried: calls = %d, want 2", c)
	}
	if time.Since(start) > time.Second {
		t.Error("took far too long, the timeout does not work")
	}
}

func TestBreakerOpensAndBlocksCalls(t *testing.T) {
	fp := &fakeProvider{chat: func(ctx context.Context, n int) (*ChatResponse, error) { return nil, status(503) }}
	r := NewResilient(fp, testCfg(time.Second, 1, 3, time.Hour)) // no retries, opens after 3

	for i := 0; i < 3; i++ {
		r.Chat(context.Background(), anyRequest)
	}
	if r.BreakerState() != breaker.Open {
		t.Fatalf("breaker should be open, state %v", r.BreakerState())
	}

	_, err := r.Chat(context.Background(), anyRequest)
	if !errors.Is(err, breaker.ErrOpen) {
		t.Errorf("expected ErrOpen, got %v", err)
	}
	if c, _ := fp.calls(); c != 3 {
		t.Errorf("the 4th call must not reach the provider, calls = %d", c)
	}
}

func TestRetriesStopWhenTheBreakerOpens(t *testing.T) {
	fp := &fakeProvider{chat: func(ctx context.Context, n int) (*ChatResponse, error) { return nil, status(503) }}
	r := NewResilient(fp, testCfg(time.Second, 5, 2, time.Hour)) // 5 attempts allowed, breaker opens at 2

	_, err := r.Chat(context.Background(), anyRequest)
	if !errors.Is(err, breaker.ErrOpen) {
		t.Errorf("expected ErrOpen, got %v", err)
	}
	if c, _ := fp.calls(); c != 2 {
		t.Errorf("provider calls = %d, want 2 (the breaker stopped the rest)", c)
	}
}

func TestBreakerRecoversWhenProviderIsBack(t *testing.T) {
	healthy := false
	var mu sync.Mutex
	fp := &fakeProvider{chat: func(ctx context.Context, n int) (*ChatResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		if healthy {
			return okResponse(), nil
		}
		return nil, status(503)
	}}
	r := NewResilient(fp, testCfg(time.Second, 1, 2, 50*time.Millisecond))

	r.Chat(context.Background(), anyRequest)
	r.Chat(context.Background(), anyRequest)
	if r.BreakerState() != breaker.Open {
		t.Fatal("should be open")
	}

	mu.Lock()
	healthy = true
	mu.Unlock()
	time.Sleep(80 * time.Millisecond) // cooldown is over

	if _, err := r.Chat(context.Background(), anyRequest); err != nil {
		t.Fatalf("the test call should work: %v", err)
	}
	if r.BreakerState() != breaker.Closed {
		t.Errorf("breaker should be closed again, state %v", r.BreakerState())
	}
}

// A client that leaves says nothing about the provider's health.
func TestClientLeavingDoesNotTripTheBreaker(t *testing.T) {
	fp := &fakeProvider{chat: func(ctx context.Context, n int) (*ChatResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	r := NewResilient(fp, testCfg(time.Second, 1, 1, time.Hour)) // opens after ONE failure

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r.Chat(ctx, anyRequest)
	}
	if r.BreakerState() != breaker.Closed {
		t.Errorf("cancelled calls must not count as failures, state %v", r.BreakerState())
	}
}

func TestBadRequestsDoNotTripTheBreaker(t *testing.T) {
	fp := &fakeProvider{chat: func(ctx context.Context, n int) (*ChatResponse, error) { return nil, status(401) }}
	r := NewResilient(fp, testCfg(time.Second, 1, 1, time.Hour))

	for i := 0; i < 5; i++ {
		r.Chat(context.Background(), anyRequest)
	}
	if r.BreakerState() != breaker.Closed {
		t.Error("a 401 means the provider is alive, the breaker must stay closed")
	}
}

// ---- streaming ----

func TestStreamStartIsRetried(t *testing.T) {
	fp := &fakeProvider{stream: func(ctx context.Context, n int) (<-chan StreamChunk, error) {
		if n == 1 {
			return nil, status(503)
		}
		return streamOf(StreamChunk{Content: "hi"}, StreamChunk{FinishReason: "stop"}), nil
	}}
	r := NewResilient(fp, testCfg(time.Second, 3, 10, time.Second))

	ch, err := r.ChatStream(context.Background(), anyRequest)
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)
	if len(chunks) != 2 || chunks[0].Content != "hi" {
		t.Errorf("wrong chunks: %+v", chunks)
	}
	if _, s := fp.calls(); s != 2 {
		t.Errorf("stream calls = %d, want 2", s)
	}
}

func TestStreamStartTimeout(t *testing.T) {
	fp := &fakeProvider{stream: func(ctx context.Context, n int) (<-chan StreamChunk, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	r := NewResilient(fp, testCfg(30*time.Millisecond, 2, 10, time.Second))

	_, err := r.ChatStream(context.Background(), anyRequest)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected a timeout error, got %v", err)
	}
	if _, s := fp.calls(); s != 2 {
		t.Errorf("a slow start is retried: calls = %d, want 2", s)
	}
}

// The timeout is only for the start. A long stream that is healthy must not be cut.
func TestStartedStreamIsNotCutByTheTimeout(t *testing.T) {
	fp := &fakeProvider{stream: func(ctx context.Context, n int) (<-chan StreamChunk, error) {
		ch := make(chan StreamChunk)
		go func() {
			defer close(ch)
			for i := 0; i < 5; i++ {
				time.Sleep(30 * time.Millisecond) // 150ms in total, the timeout is only 50ms
				if !sendChunk(ctx, ch, StreamChunk{Content: "x"}) {
					return
				}
			}
		}()
		return ch, nil
	}}
	r := NewResilient(fp, testCfg(50*time.Millisecond, 1, 10, time.Second))

	ch, err := r.ChatStream(context.Background(), anyRequest)
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)
	if len(chunks) != 5 {
		t.Errorf("got %d chunks, want 5, the stream was cut by the timeout", len(chunks))
	}
	for _, c := range chunks {
		if c.Err != nil {
			t.Errorf("unexpected error chunk: %v", c.Err)
		}
	}
}

// waitForState waits until the breaker reaches a state (the stream result arrives a moment later).
func waitForState(t *testing.T, r *Resilient, want breaker.State) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for r.BreakerState() != want {
		if time.Now().After(deadline) {
			t.Fatalf("breaker state is %v, wanted %v", r.BreakerState(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMidStreamFailureIsNotRetriedButCounts(t *testing.T) {
	fp := &fakeProvider{stream: func(ctx context.Context, n int) (<-chan StreamChunk, error) {
		return streamOf(StreamChunk{Content: "he"}, StreamChunk{Err: errors.New("connection reset")}), nil
	}}
	r := NewResilient(fp, testCfg(time.Second, 3, 1, time.Hour)) // opens after one failure

	ch, err := r.ChatStream(context.Background(), anyRequest)
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)
	if len(chunks) != 2 || chunks[1].Err == nil {
		t.Fatalf("the error must reach the caller as it is: %+v", chunks)
	}
	if _, s := fp.calls(); s != 1 {
		t.Errorf("a broken stream must not be retried (the client already got data): calls = %d", s)
	}
	waitForState(t, r, breaker.Open) // but the breaker learned about it
}

func TestStreamClientLeavingStopsProviderAndIsNotAFailure(t *testing.T) {
	providerStopped := make(chan struct{})
	fp := &fakeProvider{stream: func(ctx context.Context, n int) (<-chan StreamChunk, error) {
		ch := make(chan StreamChunk)
		go func() {
			defer close(providerStopped)
			defer close(ch)
			for {
				if !sendChunk(ctx, ch, StreamChunk{Content: "x"}) {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
		return ch, nil
	}}
	r := NewResilient(fp, testCfg(time.Second, 1, 1, time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := r.ChatStream(ctx, anyRequest)
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	cancel() // the client leaves

	select {
	case <-providerStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("the provider was not stopped after the client left")
	}
	collect(t, ch) // our channel closes too
	time.Sleep(30 * time.Millisecond)
	if r.BreakerState() != breaker.Closed {
		t.Errorf("a client that leaves is not a provider failure, state %v", r.BreakerState())
	}
}

func TestStreamIsBlockedWhenBreakerIsOpen(t *testing.T) {
	fp := &fakeProvider{stream: func(ctx context.Context, n int) (<-chan StreamChunk, error) { return nil, status(503) }}
	r := NewResilient(fp, testCfg(time.Second, 1, 1, time.Hour))

	r.ChatStream(context.Background(), anyRequest) // fails, opens
	_, err := r.ChatStream(context.Background(), anyRequest)
	if !errors.Is(err, breaker.ErrOpen) {
		t.Errorf("expected ErrOpen, got %v", err)
	}
	if _, s := fp.calls(); s != 1 {
		t.Errorf("stream calls = %d, want 1", s)
	}
}

func TestSuccessfulStreamKeepsBreakerClosed(t *testing.T) {
	fp := &fakeProvider{stream: func(ctx context.Context, n int) (<-chan StreamChunk, error) {
		return streamOf(StreamChunk{Content: "a"}, StreamChunk{FinishReason: "stop"}), nil
	}}
	r := NewResilient(fp, testCfg(time.Second, 1, 1, time.Hour))

	ch, _ := r.ChatStream(context.Background(), anyRequest)
	collect(t, ch)
	time.Sleep(20 * time.Millisecond)
	if r.BreakerState() != breaker.Closed {
		t.Errorf("state %v, want closed", r.BreakerState())
	}
}

// A client that leaves during the half-open test call proves nothing about the provider.
// The breaker must stay half-open and not close on a test call that never finished.
func TestClientLeavingDuringTestCallDoesNotCloseTheBreaker(t *testing.T) {
	fp := &fakeProvider{stream: func(ctx context.Context, n int) (<-chan StreamChunk, error) {
		if n == 1 {
			return nil, status(503) // the first call fails and opens the breaker
		}
		ch := make(chan StreamChunk)
		go func() {
			defer close(ch)
			sendChunk(ctx, ch, StreamChunk{Content: "x"})
			<-ctx.Done() // then it waits, the stream is still running
		}()
		return ch, nil
	}}
	r := NewResilient(fp, testCfg(time.Second, 1, 1, 20*time.Millisecond))

	r.ChatStream(context.Background(), anyRequest) // opens the breaker
	time.Sleep(40 * time.Millisecond)              // cooldown is over

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := r.ChatStream(ctx, anyRequest) // this is the test call
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	cancel() // the client leaves
	collect(t, ch)
	time.Sleep(30 * time.Millisecond)

	if r.BreakerState() != breaker.HalfOpen {
		t.Errorf("state is %v, want half-open: the test call never finished", r.BreakerState())
	}
}
