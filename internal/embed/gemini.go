package embed

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

// Gemini's OpenAI-compatible base URL. The embeddings path is /embeddings, the key goes in
// "Authorization: Bearer" (same as for chat). Docs: https://ai.google.dev/gemini-api/docs/openai
const (
	GeminiBaseURL = "https://generativelanguage.googleapis.com/v1beta/openai"
	GeminiModel   = "gemini-embedding-001"

	maxEmbedResponse = 4 << 20 // an answer has 3072 numbers (about 60 KB), 4 MB is plenty
)

// OpenAICompatible calls POST {baseURL}/embeddings.
type OpenAICompatible struct {
	baseURL string
	apiKey  string
	model   string
	dims    int
	client  *http.Client
}

func NewOpenAICompatible(baseURL, apiKey, model string, dims int, timeout time.Duration) *OpenAICompatible {
	return &OpenAICompatible{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey, model: model, dims: dims,
		client: &http.Client{Timeout: timeout},
	}
}

func (e *OpenAICompatible) Dimensions() int { return e.dims }

// The request and response of the OpenAI embeddings API.
type embedRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}
type embedResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

func (e *OpenAICompatible) Embed(ctx context.Context, text string) ([]float32, error) {
	body, err := json.Marshal(embedRequest{Model: e.model, Input: text})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+e.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding request failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxEmbedResponse))
	if err != nil {
		return nil, fmt.Errorf("reading embedding response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		// Only the status goes into the error: the body could contain parts of the prompt.
		return nil, fmt.Errorf("embedding provider returned status %d", resp.StatusCode)
	}

	var out embedResponse
	if err := json.Unmarshal(data, &out); err != nil || len(out.Data) == 0 {
		return nil, fmt.Errorf("embedding response has an unexpected shape")
	}
	vec := out.Data[0].Embedding
	if len(vec) < e.dims {
		return nil, fmt.Errorf("embedding has %d numbers, expected at least %d", len(vec), e.dims)
	}
	return fit(vec, e.dims), nil
}
