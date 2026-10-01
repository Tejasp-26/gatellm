package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"gatellm/internal/provider"
)

// keyInput lists everything that can change the answer.
// "stream" and "stream_options" are not here: they change how the answer is delivered, not what it says.
type keyInput struct {
	Tenant      string             `json:"tenant"`
	Model       string             `json:"model"`
	Messages    []provider.Message `json:"messages"`
	Temperature *float64           `json:"temperature"`
	MaxTokens   int                `json:"max_tokens"`
}

// Key builds the Redis key for a request: "cache:<tenant>:<sha256 of the inputs>".
//
// model is the model name EXACTLY as the client sent it (for example "auto" or "groq/llama-3.1-8b-instant"),
// because "auto" and "groq/..." can give different answers.
// The tenant is part of the key, so one customer can never receive another customer's answer.
func Key(tenantID, model string, req *provider.ChatRequest) string {
	// A struct is always written in the same field order, so the same input gives the same bytes.
	data, _ := json.Marshal(keyInput{
		Tenant:      tenantID,
		Model:       model,
		Messages:    req.Messages,
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
	})
	sum := sha256.Sum256(data)
	return "cache:" + tenantID + ":" + hex.EncodeToString(sum[:])
}
