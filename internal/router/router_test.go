package router

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gatellm/internal/breaker"
	"gatellm/internal/provider"
)

// fake is a provider that the test controls.
type fake struct {
	name  string
	state breaker.State // what BreakerState() reports

	mu        sync.Mutex
	chatErr   error
	streamErr error
	chunks    []provider.StreamChunk // what a started stream delivers
	delay     time.Duration
	pingErr   error

	chatCalls  atomic.Int64
	pings      atomic.Int64
	modelsSeen []string // the model names this provider received
}

func (f *fake) Name() string                { return f.name }
func (f *fake) BreakerState() breaker.State { return f.state }

func (f *fake) Chat(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	f.chatCalls.Add(1)
	f.mu.Lock()
	f.modelsSeen = append(f.modelsSeen, req.Model)
	err, delay := f.chatErr, f.delay
	f.mu.Unlock()
	time.Sleep(delay)
	if err != nil {
		return nil, err
	}
	return &provider.ChatResponse{Choices: []provider.Choice{{Message: provider.Message{Content: "from " + f.name}}}}, nil
}

func (f *fake) ChatStream(ctx context.Context, req *provider.ChatRequest) (<-chan provider.StreamChunk, error) {
	f.mu.Lock()
	err, chunks := f.streamErr, f.chunks
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	ch := make(chan provider.StreamChunk, len(chunks)+1)
	for _, c := range chunks {
		ch <- c
	}
	ch <- provider.StreamChunk{Content: "from " + f.name}
	close(ch)
	return ch, nil
}

func (f *fake) Ping(ctx context.Context) error {
	f.pings.Add(1)
	return f.pingErr
}

func status(code int) error { return &provider.ProviderError{StatusCode: code} }

// newRouter builds a router with the priority strategy. The model of each target is "<name>-model".
func newRouter(fakes ...*fake) *Router {
	var targets []*Target
	for _, f := range fakes {
		targets = append(targets, &Target{Provider: f, Model: f.name + "-model", Weight: 1})
	}
	s, _ := NewStrategy("priority")
	return New(targets, s)
}

var anyReq = &provider.ChatRequest{Model: "auto", Messages: []provider.Message{{Role: "user", Content: "hi"}}}

func answer(resp *provider.ChatResponse) string { return resp.Choices[0].Message.Content }

func TestFirstTargetAnswers(t *testing.T) {
	a, b := &fake{name: "a"}, &fake{name: "b"}
	resp, served, err := newRouter(a, b).Chat(context.Background(), anyReq)
	if err != nil || answer(resp) != "from a" {
		t.Fatalf("a should answer: %v", err)
	}
	if served.Provider != "a" || served.Model != "a-model" || len(served.Tried) != 1 {
		t.Errorf("wrong served info: %+v", served)
	}
	if b.chatCalls.Load() != 0 {
		t.Error("b must not be called when a works")
	}
}

func TestFallsBackToTheNextProvider(t *testing.T) {
	a, b := &fake{name: "a", chatErr: status(503)}, &fake{name: "b"}
	resp, served, err := newRouter(a, b).Chat(context.Background(), anyReq)
	if err != nil || answer(resp) != "from b" {
		t.Fatalf("b should answer after a failed: %v", err)
	}
	if served.Provider != "b" || served.Model != "b-model" {
		t.Errorf("wrong served info: %+v", served)
	}
	if len(served.Tried) != 2 || served.Tried[0] != "a" || served.Tried[1] != "b" {
		t.Errorf("tried = %v, want [a b]", served.Tried)
	}
}

func TestEachTargetGetsItsOwnModelAndTheRequestIsNotChanged(t *testing.T) {
	a, b := &fake{name: "a", chatErr: status(503)}, &fake{name: "b"}
	req := &provider.ChatRequest{Model: "auto", Messages: []provider.Message{{Role: "user", Content: "hi"}}}
	newRouter(a, b).Chat(context.Background(), req)

	if a.modelsSeen[0] != "a-model" || b.modelsSeen[0] != "b-model" {
		t.Errorf("models seen: a=%v b=%v", a.modelsSeen, b.modelsSeen)
	}
	if req.Model != "auto" {
		t.Errorf("the caller's request was changed: model = %q", req.Model)
	}
}

func TestFallsBackOnManyKindsOfFailure(t *testing.T) {
	for name, err := range map[string]error{
		"429":          status(429),
		"500":          status(500),
		"401 bad key":  status(401),
		"404 no model": status(404),
		"timeout":      context.DeadlineExceeded,
		"breaker open": breaker.ErrOpen,
		"network":      errors.New("connection refused"),
	} {
		t.Run(name, func(t *testing.T) {
			a, b := &fake{name: "a", chatErr: err}, &fake{name: "b"}
			resp, _, e := newRouter(a, b).Chat(context.Background(), anyReq)
			if e != nil || answer(resp) != "from b" {
				t.Errorf("should fall back, got %v", e)
			}
		})
	}
}

func TestDoesNotFallBackOnABadRequest(t *testing.T) {
	for _, code := range []int{400, 413, 422} {
		a, b := &fake{name: "a", chatErr: status(code)}, &fake{name: "b"}
		_, served, err := newRouter(a, b).Chat(context.Background(), anyReq)

		var pe *provider.ProviderError
		if !errors.As(err, &pe) || pe.StatusCode != code {
			t.Errorf("%d: the error should come back as it is, got %v", code, err)
		}
		if b.chatCalls.Load() != 0 || len(served.Tried) != 1 {
			t.Errorf("%d: a bad request must not be sent to other providers", code)
		}
	}
}

func TestDoesNotFallBackWhenTheClientLeft(t *testing.T) {
	a, b := &fake{name: "a", chatErr: context.Canceled}, &fake{name: "b"}
	_, _, err := newRouter(a, b).Chat(context.Background(), anyReq)
	if !errors.Is(err, context.Canceled) || b.chatCalls.Load() != 0 {
		t.Errorf("a cancelled request must stop at once, err %v", err)
	}
}

func TestAllTargetsFail(t *testing.T) {
	a, b := &fake{name: "a", chatErr: status(503)}, &fake{name: "b", chatErr: status(502)}
	_, served, err := newRouter(a, b).Chat(context.Background(), anyReq)

	var pe *provider.ProviderError
	if !errors.As(err, &pe) || pe.StatusCode != 502 {
		t.Errorf("the LAST error should come back, got %v", err)
	}
	if len(served.Tried) != 2 {
		t.Errorf("tried = %v, want both", served.Tried)
	}
}

func TestTargetWithOpenBreakerGoesToTheBack(t *testing.T) {
	a, b := &fake{name: "a", state: breaker.Open}, &fake{name: "b"}
	resp, served, _ := newRouter(a, b).Chat(context.Background(), anyReq)

	if answer(resp) != "from b" || served.Tried[0] != "b" {
		t.Errorf("b should be tried first, tried = %v", served.Tried)
	}
	if a.chatCalls.Load() != 0 {
		t.Error("a is down and b works, so a must not be called")
	}
}

func TestDownTargetIsStillTriedLastWhenTheOthersFail(t *testing.T) {
	a := &fake{name: "a", state: breaker.Open}
	b := &fake{name: "b", chatErr: status(503)}
	_, served, _ := newRouter(a, b).Chat(context.Background(), anyReq)

	if len(served.Tried) != 2 || served.Tried[0] != "b" || served.Tried[1] != "a" {
		t.Errorf("tried = %v, want [b a]", served.Tried)
	}
}

func TestHalfOpenTargetKeepsItsPlace(t *testing.T) {
	a, b := &fake{name: "a", state: breaker.HalfOpen}, &fake{name: "b"}
	_, served, _ := newRouter(a, b).Chat(context.Background(), anyReq)
	if served.Provider != "a" {
		t.Errorf("a is half-open and gets the test call, served by %s", served.Provider)
	}
}

func TestLatencyIsRecordedOnSuccessOnly(t *testing.T) {
	a := &fake{name: "a", delay: 20 * time.Millisecond}
	r := newRouter(a)
	r.Chat(context.Background(), anyReq)
	if got := r.targets[0].AvgLatency(); got < 15 {
		t.Errorf("latency = %vms, want about 20", got)
	}

	f := &fake{name: "f", chatErr: status(503), delay: 20 * time.Millisecond}
	r2 := newRouter(f)
	r2.Chat(context.Background(), anyReq)
	if r2.targets[0].AvgLatency() != 0 {
		t.Error("a failed call must not count as a latency measurement")
	}
}

// ---- streaming ----

func drain(ch <-chan provider.StreamChunk) []provider.StreamChunk {
	var out []provider.StreamChunk
	for c := range ch {
		out = append(out, c)
	}
	return out
}

func TestStreamFallsBackWhenItCannotStart(t *testing.T) {
	a, b := &fake{name: "a", streamErr: status(503)}, &fake{name: "b"}
	ch, served, err := newRouter(a, b).ChatStream(context.Background(), anyReq)
	if err != nil {
		t.Fatal(err)
	}
	chunks := drain(ch)
	if served.Provider != "b" || len(chunks) != 1 || chunks[0].Content != "from b" {
		t.Errorf("b should serve the stream: %+v %+v", served, chunks)
	}
}

func TestStreamFailureInTheMiddleIsNotRetriedElsewhere(t *testing.T) {
	broken := []provider.StreamChunk{{Content: "he"}, {Err: errors.New("cut off")}}
	a, b := &fake{name: "a", chunks: broken}, &fake{name: "b"}
	ch, served, err := newRouter(a, b).ChatStream(context.Background(), anyReq)
	if err != nil {
		t.Fatal(err)
	}
	chunks := drain(ch)
	if served.Provider != "a" || len(served.Tried) != 1 {
		t.Errorf("once a stream has started there is no fallback: %+v", served)
	}
	if chunks[1].Err == nil {
		t.Errorf("the error should reach the caller: %+v", chunks)
	}
}

func TestStreamDoesNotFallBackOnABadRequest(t *testing.T) {
	a, b := &fake{name: "a", streamErr: status(400)}, &fake{name: "b"}
	_, served, err := newRouter(a, b).ChatStream(context.Background(), anyReq)
	if err == nil || len(served.Tried) != 1 {
		t.Errorf("a 400 must not be tried elsewhere: err %v, served %+v", err, served)
	}
}

func TestStatus(t *testing.T) {
	a := &fake{name: "a", state: breaker.Open}
	r := newRouter(a)
	st := r.Status()
	if len(st) != 1 || st[0].Provider != "a" || st[0].Model != "a-model" || st[0].Breaker != "open" {
		t.Errorf("wrong status: %+v", st)
	}
}
