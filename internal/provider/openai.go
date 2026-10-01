package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

	maxResponseBytes = 5 << 20 // never read more than 5 MB from a provider in one answer
)

// OpenAICompatible talks to any provider that follows the OpenAI chat API.
type OpenAICompatible struct {
	name    string
	baseURL string
	apiKey  string

	// client is for normal calls. It has a total timeout.
	client *http.Client
	// streamClient is for streaming. A total timeout would cut long answers,
	// so it only limits the time to wait for the first response headers.
	streamClient *http.Client

	// If true, we ask the provider to send token usage at the end of a stream
	// (stream_options.include_usage). The Gemini docs show this option. The Groq docs list
	// stream_options but do not describe include_usage, so it is off for Groq for now.
	streamUsage bool
}

func NewOpenAICompatible(name, baseURL, apiKey string) *OpenAICompatible {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second

	return &OpenAICompatible{
		name:    name,
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		// Safety timeouts. Phase 5 adds proper per-provider timeouts.
		client:       &http.Client{Timeout: 60 * time.Second},
		streamClient: &http.Client{Transport: transport},
	}
}

func NewGroq(apiKey string) *OpenAICompatible {
	return NewOpenAICompatible("groq", groqBaseURL, apiKey)
}

func NewGemini(apiKey string) *OpenAICompatible {
	p := NewOpenAICompatible("gemini", geminiBaseURL, apiKey)
	p.streamUsage = true
	return p
}

func (p *OpenAICompatible) Name() string { return p.name }

// Ping is the health check: it asks the provider for its list of models (GET /models).
// Groq and Gemini both offer this in their OpenAI-compatible API (checked in the docs).
// It generates no text, so it uses no tokens.
func (p *OpenAICompatible) Ping(ctx context.Context) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/models", nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return errorFromResponse(resp)
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes)) // read the body so the connection can be reused
	return nil
}

// newRequest builds the HTTP request to the provider.
func (p *OpenAICompatible) newRequest(ctx context.Context, body *ChatRequest) (*http.Request, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey) // the key is never logged
	return httpReq, nil
}

// errorFromResponse turns an HTTP error answer into a ProviderError.
func errorFromResponse(resp *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	msg := string(data)
	if len(msg) > 200 {
		msg = msg[:200] // keep logs short
	}
	return &ProviderError{StatusCode: resp.StatusCode, Message: msg}
}

func (p *OpenAICompatible) Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	// Copy the request, so we never change the caller's data.
	body := *req
	body.Stream = false
	body.StreamOptions = nil // providers reject stream_options when stream is false

	httpReq, err := p.newRequest(ctx, &body)
	if err != nil {
		return nil, err
	}

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("call %s: %w", p.name, err) // network error or timeout
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errorFromResponse(resp)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("read %s response: %w", p.name, err)
	}
	var out ChatResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode %s response: %w", p.name, err)
	}
	return &out, nil
}

// streamEvent is the part of one streamed JSON event that we care about.
type streamEvent struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *Usage `json:"usage"`
	XGroq *struct {
		Usage *Usage `json:"usage"`
	} `json:"x_groq"` // Groq may report usage here
}

// ChatStream calls the provider with stream:true and turns its Server-Sent Events into chunks.
func (p *OpenAICompatible) ChatStream(ctx context.Context, req *ChatRequest) (<-chan StreamChunk, error) {
	body := *req
	body.Stream = true
	body.StreamOptions = nil
	if p.streamUsage {
		body.StreamOptions = &StreamOptions{IncludeUsage: true}
	}

	httpReq, err := p.newRequest(ctx, &body)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Accept", "text/event-stream")

	resp, err := p.streamClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("call %s: %w", p.name, err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, errorFromResponse(resp)
	}

	ch := make(chan StreamChunk)
	go func() {
		defer close(ch)
		defer resp.Body.Close() // closing the body also frees the connection

		// A streamed line can be long, so we allow a bigger buffer than the default.
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 64*1024), 1<<20)

		finished := false // true once we saw a finish_reason or [DONE]
		for scanner.Scan() {
			// SSE lines look like "data: {...}". Blank lines and ": comment" lines are skipped.
			data, ok := strings.CutPrefix(scanner.Text(), "data:")
			if !ok {
				continue
			}
			data = strings.TrimSpace(data)
			if data == "[DONE]" {
				return
			}

			var ev streamEvent
			if err := json.Unmarshal([]byte(data), &ev); err != nil {
				sendChunk(ctx, ch, StreamChunk{Err: fmt.Errorf("bad data in %s stream: %w", p.name, err)})
				return
			}

			chunk := StreamChunk{Usage: ev.Usage}
			if chunk.Usage == nil && ev.XGroq != nil {
				chunk.Usage = ev.XGroq.Usage
			}
			if len(ev.Choices) > 0 {
				chunk.Content = ev.Choices[0].Delta.Content
				if fr := ev.Choices[0].FinishReason; fr != nil && *fr != "" {
					chunk.FinishReason = *fr
					finished = true
				}
			}
			if chunk.Content == "" && chunk.FinishReason == "" && chunk.Usage == nil {
				continue // nothing useful in this event
			}
			if !sendChunk(ctx, ch, chunk) {
				return
			}
		}

		// The loop ended without [DONE].
		if ctx.Err() != nil {
			return // we cancelled it ourselves (the client left), this is not an error
		}
		if err := scanner.Err(); err != nil {
			sendChunk(ctx, ch, StreamChunk{Err: fmt.Errorf("read %s stream: %w", p.name, err)})
			return
		}
		if !finished {
			sendChunk(ctx, ch, StreamChunk{Err: errors.New(p.name + " stream ended before the answer was complete")})
		}
	}()
	return ch, nil
}
