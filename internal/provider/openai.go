package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Groq and Gemini both offer an OpenAI-compatible API (checked in their official docs).
// So one adapter class works for both. Only the name, base URL and key are different.
const (
	groqBaseURL   = "https://api.groq.com/openai/v1"
	geminiBaseURL = "https://generativelanguage.googleapis.com/v1beta/openai"

	maxResponseBytes = 5 << 20 // never read more than 5 MB from a provider
)

// OpenAICompatible talks to any provider that follows the OpenAI chat API.
type OpenAICompatible struct {
	name    string
	baseURL string
	apiKey  string
	client  *http.Client
}

func NewOpenAICompatible(name, baseURL, apiKey string) *OpenAICompatible {
	return &OpenAICompatible{
		name:    name,
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		// Safety timeout. Phase 5 adds proper per-provider timeouts.
		client: &http.Client{Timeout: 60 * time.Second},
	}
}

func NewGroq(apiKey string) *OpenAICompatible {
	return NewOpenAICompatible("groq", groqBaseURL, apiKey)
}

func NewGemini(apiKey string) *OpenAICompatible {
	return NewOpenAICompatible("gemini", geminiBaseURL, apiKey)
}

func (p *OpenAICompatible) Name() string { return p.name }

func (p *OpenAICompatible) Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	// Copy the request, so we never change the caller's data.
	body := *req
	body.Stream = false

	payload, err := json.Marshal(&body)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey) // the key is never logged

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("call %s: %w", p.name, err) // network error or timeout
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("read %s response: %w", p.name, err)
	}

	if resp.StatusCode != http.StatusOK {
		msg := string(data)
		if len(msg) > 200 {
			msg = msg[:200] // keep logs short
		}
		return nil, &ProviderError{StatusCode: resp.StatusCode, Message: msg}
	}

	var out ChatResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode %s response: %w", p.name, err)
	}
	return &out, nil
}

func (p *OpenAICompatible) ChatStream(ctx context.Context, req *ChatRequest) (<-chan StreamChunk, error) {
	return nil, ErrStreamingNotReady
}
