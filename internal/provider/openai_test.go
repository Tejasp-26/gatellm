package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// We start a fake "Groq" server on our own machine, so the tests need no internet or key.

func TestOpenAICompatibleSuccess(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that we call the right path with the right key.
		if r.URL.Path != "/chat/completions" {
			t.Errorf("wrong path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("wrong auth header: %q", r.Header.Get("Authorization"))
		}
		var got ChatRequest
		json.NewDecoder(r.Body).Decode(&got)
		if got.Model != "llama-test" || got.Stream {
			t.Errorf("wrong body: %+v", got)
		}
		if got.StreamOptions != nil {
			t.Error("stream_options must not be sent when stream is false")
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"abc","object":"chat.completion","created":1,"model":"llama-test",
			"choices":[{"index":0,"message":{"role":"assistant","content":"Hi there"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`))
	}))
	defer fake.Close()

	p := NewOpenAICompatible("groq", fake.URL, "test-key")
	resp, err := p.Chat(context.Background(), &ChatRequest{
		Model:         "llama-test",
		Messages:      []Message{{Role: "user", Content: "hi"}},
		StreamOptions: &StreamOptions{IncludeUsage: true}, // must be dropped by the adapter
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Choices[0].Message.Content != "Hi there" || resp.Usage.TotalTokens != 5 {
		t.Errorf("wrong response: %+v", resp)
	}
}

func TestOpenAICompatibleHTTPError(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer fake.Close()

	p := NewOpenAICompatible("groq", fake.URL, "test-key")
	_, err := p.Chat(context.Background(), &ChatRequest{Model: "x", Messages: []Message{{Role: "user", Content: "hi"}}})

	var pe *ProviderError
	if !errors.As(err, &pe) || pe.StatusCode != 429 {
		t.Errorf("expected ProviderError 429, got %v", err)
	}
}

// ---- streaming ----

// Example events, in the format the providers send.
const (
	evRole   = `data: {"choices":[{"delta":{"role":"assistant","content":""},"finish_reason":null}]}` + "\n\n"
	evHel    = `data: {"choices":[{"delta":{"content":"Hel"},"finish_reason":null}]}` + "\n\n"
	evLo     = `data: {"choices":[{"delta":{"content":"lo"},"finish_reason":null}]}` + "\n\n"
	evFinish = `data: {"choices":[{"delta":{},"finish_reason":"stop"}]}` + "\n\n"
	evUsage  = `data: {"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}` + "\n\n"
	evDone   = "data: [DONE]\n\n"
)

// sseServer starts a fake provider that streams. write sends raw text to the client and flushes it.
func sseServer(handler func(w http.ResponseWriter, r *http.Request, write func(string))) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		handler(w, r, func(s string) {
			w.Write([]byte(s))
			flusher.Flush()
		})
	}))
}

func streamRequest() *ChatRequest {
	return &ChatRequest{Model: "llama-test", Messages: []Message{{Role: "user", Content: "hi"}}}
}

func TestOpenAIStreamSuccess(t *testing.T) {
	fake := sseServer(func(w http.ResponseWriter, r *http.Request, write func(string)) {
		var got ChatRequest
		json.NewDecoder(r.Body).Decode(&got)
		if !got.Stream || got.StreamOptions == nil || !got.StreamOptions.IncludeUsage {
			t.Errorf("expected stream and include_usage in the body, got %+v", got)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("wrong auth header: %q", r.Header.Get("Authorization"))
		}

		write(": keep-alive\n\n") // a comment line must be ignored
		write(evRole)
		write(evHel)
		write(evLo)
		write(evFinish)
		write(evUsage)
		write(evDone)
	})
	defer fake.Close()

	p := NewOpenAICompatible("gemini", fake.URL, "test-key")
	p.streamUsage = true

	ch, err := p.ChatStream(context.Background(), streamRequest())
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)

	text, finish := "", ""
	var usage *Usage
	for _, c := range chunks {
		if c.Err != nil {
			t.Fatalf("unexpected error chunk: %v", c.Err)
		}
		text += c.Content
		if c.FinishReason != "" {
			finish = c.FinishReason
		}
		if c.Usage != nil {
			usage = c.Usage
		}
	}
	if text != "Hello" || finish != "stop" {
		t.Errorf("wrong result: text %q finish %q", text, finish)
	}
	if usage == nil || usage.TotalTokens != 5 {
		t.Errorf("usage not captured: %+v", usage)
	}
}

func TestOpenAIStreamSendsNoUsageOptionByDefault(t *testing.T) {
	fake := sseServer(func(w http.ResponseWriter, r *http.Request, write func(string)) {
		var got ChatRequest
		json.NewDecoder(r.Body).Decode(&got)
		if got.StreamOptions != nil {
			t.Errorf("stream_options should not be sent by default, got %+v", got.StreamOptions)
		}
		write(evHel)
		write(evFinish)
		write(evDone)
	})
	defer fake.Close()

	p := NewOpenAICompatible("groq", fake.URL, "test-key") // streamUsage is off
	ch, err := p.ChatStream(context.Background(), streamRequest())
	if err != nil {
		t.Fatal(err)
	}
	collect(t, ch)
}

func TestOpenAIStreamReadsGroqUsage(t *testing.T) {
	fake := sseServer(func(w http.ResponseWriter, r *http.Request, write func(string)) {
		write(evHel)
		write(`data: {"choices":[{"delta":{},"finish_reason":"stop"}],"x_groq":{"usage":{"prompt_tokens":4,"completion_tokens":6,"total_tokens":10}}}` + "\n\n")
		write(evDone)
	})
	defer fake.Close()

	p := NewOpenAICompatible("groq", fake.URL, "test-key")
	ch, err := p.ChatStream(context.Background(), streamRequest())
	if err != nil {
		t.Fatal(err)
	}
	var usage *Usage
	for _, c := range collect(t, ch) {
		if c.Usage != nil {
			usage = c.Usage
		}
	}
	if usage == nil || usage.TotalTokens != 10 {
		t.Errorf("x_groq usage not read: %+v", usage)
	}
}

func TestOpenAIStreamErrorAtStart(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer fake.Close()

	p := NewOpenAICompatible("groq", fake.URL, "test-key")
	ch, err := p.ChatStream(context.Background(), streamRequest())

	var pe *ProviderError
	if !errors.As(err, &pe) || pe.StatusCode != 429 || ch != nil {
		t.Errorf("expected ProviderError 429 and no channel, got %v, %v", err, ch)
	}
}

func TestOpenAIStreamCutOffIsAnError(t *testing.T) {
	fake := sseServer(func(w http.ResponseWriter, r *http.Request, write func(string)) {
		write(evHel)
		// The server ends here: no finish_reason and no [DONE].
	})
	defer fake.Close()

	p := NewOpenAICompatible("groq", fake.URL, "test-key")
	ch, err := p.ChatStream(context.Background(), streamRequest())
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)

	last := chunks[len(chunks)-1]
	if last.Err == nil {
		t.Errorf("a cut-off stream should end with an error chunk: %+v", chunks)
	}
}

func TestOpenAIStreamBadDataIsAnError(t *testing.T) {
	fake := sseServer(func(w http.ResponseWriter, r *http.Request, write func(string)) {
		write("data: {this is not json}\n\n")
	})
	defer fake.Close()

	p := NewOpenAICompatible("groq", fake.URL, "test-key")
	ch, err := p.ChatStream(context.Background(), streamRequest())
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)
	if len(chunks) != 1 || chunks[0].Err == nil {
		t.Errorf("expected exactly one error chunk, got %+v", chunks)
	}
}

// The important one: when our side cancels, the connection to the provider must be closed.
func TestOpenAIStreamCancelClosesUpstream(t *testing.T) {
	upstreamSawClose := make(chan struct{})
	fake := sseServer(func(w http.ResponseWriter, r *http.Request, write func(string)) {
		write(evHel)
		<-r.Context().Done() // this is only released when the client (our adapter) hangs up
		close(upstreamSawClose)
	})
	defer fake.Close()

	p := NewOpenAICompatible("groq", fake.URL, "test-key")
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := p.ChatStream(ctx, streamRequest())
	if err != nil {
		t.Fatal(err)
	}

	first := <-ch
	if first.Content != "Hel" {
		t.Fatalf("unexpected first chunk: %+v", first)
	}
	cancel() // the end client left

	select {
	case <-upstreamSawClose:
		// good: the provider call was stopped
	case <-time.After(2 * time.Second):
		t.Fatal("the upstream connection was not closed after cancel")
	}
	collect(t, ch) // and our channel closes too
}
