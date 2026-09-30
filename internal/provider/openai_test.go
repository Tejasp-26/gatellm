package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// We start a fake "Groq" server on our own machine, so the test needs no internet or key.

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

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"abc","object":"chat.completion","created":1,"model":"llama-test",
			"choices":[{"index":0,"message":{"role":"assistant","content":"Hi there"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`))
	}))
	defer fake.Close()

	p := NewOpenAICompatible("groq", fake.URL, "test-key")
	resp, err := p.Chat(context.Background(), &ChatRequest{
		Model:    "llama-test",
		Messages: []Message{{Role: "user", Content: "hi"}},
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
