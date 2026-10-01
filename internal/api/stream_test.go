package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gatellm/internal/breaker"
	"gatellm/internal/provider"
)

// newServerWith builds the real router with one provider called "mock" (or the given name).
func newServerWith(name string, p provider.Provider) http.Handler {
	h := &Handler{
		Providers:  provider.Registry{name: p},
		Tenants:    newFakeStore(),
		AdminToken: testAdminToken,
	}
	return NewRouter(h, 1<<20)
}

// events splits an SSE body into the text after each "data: ".
func events(body string) []string {
	var out []string
	for _, block := range strings.Split(body, "\n\n") {
		block = strings.TrimSpace(block)
		if data, ok := strings.CutPrefix(block, "data: "); ok {
			out = append(out, data)
		}
	}
	return out
}

func streamPost(srv http.Handler, body string) *httptest.ResponseRecorder {
	return do(srv, http.MethodPost, "/v1/chat/completions", body, "Bearer "+testAPIKey)
}

// chunkView is the part of a streamed chunk that the tests look at.
type chunkView struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Choices []struct {
		Delta struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *provider.Usage `json:"usage"`
}

func TestStreamSuccess(t *testing.T) {
	srv := newServerWith("mock", provider.NewMock(0, 0))
	rec := streamPost(srv, `{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}`)

	if rec.Code != 200 {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Content-Type") != "text/event-stream" || rec.Header().Get("X-Provider") != "mock" {
		t.Errorf("wrong headers: %v", rec.Header())
	}

	ev := events(rec.Body.String())
	if len(ev) < 3 || ev[len(ev)-1] != "[DONE]" {
		t.Fatalf("stream should end with [DONE]: %v", ev)
	}

	var text, finish, firstRole string
	var id string
	for i, e := range ev[:len(ev)-1] {
		var c chunkView
		if err := json.Unmarshal([]byte(e), &c); err != nil {
			t.Fatalf("event %d is not JSON: %q", i, e)
		}
		if c.Object != "chat.completion.chunk" || !strings.HasPrefix(c.ID, "chatcmpl-") {
			t.Errorf("wrong chunk envelope: %+v", c)
		}
		if id == "" {
			id = c.ID
		} else if c.ID != id {
			t.Error("all chunks must share one id")
		}
		if c.Usage != nil {
			t.Error("usage must not be sent unless the client asks for it")
		}
		if i == 0 {
			firstRole = c.Choices[0].Delta.Role
		}
		text += c.Choices[0].Delta.Content
		if c.Choices[0].FinishReason != nil {
			finish = *c.Choices[0].FinishReason
		}
	}
	if firstRole != "assistant" || text != "Mock reply to: hi" || finish != "stop" {
		t.Errorf("wrong stream: role %q text %q finish %q", firstRole, text, finish)
	}
}

func TestStreamIncludeUsage(t *testing.T) {
	srv := newServerWith("mock", provider.NewMock(0, 0))
	rec := streamPost(srv, `{"model":"mock","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`)

	ev := events(rec.Body.String())
	if ev[len(ev)-1] != "[DONE]" {
		t.Fatalf("stream should end with [DONE]: %v", ev)
	}

	// The event before [DONE] carries the usage and has an empty choices list.
	last := ev[len(ev)-2]
	var c chunkView
	json.Unmarshal([]byte(last), &c)
	if c.Usage == nil || c.Usage.TotalTokens == 0 || len(c.Choices) != 0 {
		t.Errorf("expected a usage chunk with empty choices, got %s", last)
	}
	if !strings.Contains(last, `"choices":[]`) {
		t.Errorf("choices must be an empty list, not null: %s", last)
	}
}

func TestStreamFailsInTheMiddle(t *testing.T) {
	srv := newServerWith("mock", provider.NewMock(0, 0))
	rec := streamPost(srv, `{"model":"mock","stream":true,"messages":[{"role":"user","content":"[fail-midstream] hi"}]}`)

	// The status was already sent, so it stays 200 ...
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	// ... the error arrives as the last event, and there is no [DONE].
	ev := events(rec.Body.String())
	last := ev[len(ev)-1]
	if !strings.Contains(last, `"error"`) || !strings.Contains(last, "upstream_error") {
		t.Errorf("last event should be an error event: %s", last)
	}
	if strings.Contains(rec.Body.String(), "[DONE]") {
		t.Error("a failed stream must not end with [DONE]")
	}
	if strings.Contains(last, "simulated") {
		t.Error("the internal error text must not reach the client")
	}
}

func TestStreamFailsAtStart(t *testing.T) {
	srv := newServerWith("mock", provider.NewMock(0, 1)) // always fails
	rec := streamPost(srv, `{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}`)

	// Nothing was sent yet, so we can still return a normal JSON error.
	if rec.Code != http.StatusBadGateway {
		t.Errorf("got status %d, want 502, body %s", rec.Code, rec.Body.String())
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		t.Errorf("wrong content type: %s", rec.Header().Get("Content-Type"))
	}
}

func TestStreamNeedsAPIKey(t *testing.T) {
	srv := newServerWith("mock", provider.NewMock(0, 0))
	rec := do(srv, http.MethodPost, "/v1/chat/completions",
		`{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "")
	if rec.Code != 401 {
		t.Errorf("got status %d, want 401", rec.Code)
	}
}

// endlessProvider streams forever, until its context is cancelled.
type endlessProvider struct {
	stopped chan struct{} // closed when the provider goroutine ends
}

func (e *endlessProvider) Name() string { return "endless" }

func (e *endlessProvider) Chat(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	return nil, errors.New("not used")
}

func (e *endlessProvider) ChatStream(ctx context.Context, req *provider.ChatRequest) (<-chan provider.StreamChunk, error) {
	ch := make(chan provider.StreamChunk)
	go func() {
		defer close(e.stopped)
		defer close(ch)
		for {
			select {
			case ch <- provider.StreamChunk{Content: "x "}:
			case <-ctx.Done():
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return ch, nil
}

// The important one: when the client leaves, the provider call must be cancelled.
func TestStreamStopsWhenClientLeaves(t *testing.T) {
	prov := &endlessProvider{stopped: make(chan struct{})}
	ts := httptest.NewServer(newServerWith("endless", prov))
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"endless/x","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer "+testAPIKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	// Read a few lines, so we know the stream is really flowing.
	reader := bufio.NewReader(resp.Body)
	for i := 0; i < 5; i++ {
		if _, err := reader.ReadString('\n'); err != nil {
			t.Fatalf("stream did not start: %v", err)
		}
	}

	cancel() // the client leaves
	resp.Body.Close()

	select {
	case <-prov.stopped:
		// good: the provider goroutine ended
	case <-time.After(3 * time.Second):
		t.Fatal("the provider was not cancelled after the client left")
	}
}

// openProvider behaves like a provider whose circuit breaker is open.
type openProvider struct{}

func (openProvider) Name() string { return "mock" }
func (openProvider) Chat(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	return nil, breaker.ErrOpen
}
func (openProvider) ChatStream(ctx context.Context, req *provider.ChatRequest) (<-chan provider.StreamChunk, error) {
	return nil, breaker.ErrOpen
}

// An open circuit gives 503 (not 502), for normal and for streaming requests.
func TestOpenCircuitGives503(t *testing.T) {
	srv := newServerWith("mock", openProvider{})
	for _, body := range []string{
		`{"model":"mock","messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}`,
	} {
		rec := streamPost(srv, body)
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status %d, want 503, body %s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "upstream_unavailable") {
			t.Errorf("wrong error type: %s", rec.Body.String())
		}
	}
}
