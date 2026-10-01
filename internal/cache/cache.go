// Package cache stores answers, so the same question does not call the provider twice.
package cache

import (
	"context"

	"gatellm/internal/provider"
)

// Entry is what we store for one answer.
type Entry struct {
	Provider string                 `json:"provider"` // who really answered, e.g. "groq"
	Model    string                 `json:"model"`    // the real model name at that provider
	Response *provider.ChatResponse `json:"response"`
}

// Cache is the one interface the handlers know. The real one is Redis (see redis.go).
// Tests use a simple fake.
type Cache interface {
	// Get returns the stored answer. hit=false (and err=nil) means "not there".
	Get(ctx context.Context, key string) (entry *Entry, hit bool, err error)
	// Set stores an answer. It is forgotten after the TTL of the cache.
	Set(ctx context.Context, key string, entry *Entry) error
}

// Cacheable says if the answer to this request may be stored and reused.
//
//	never:  streaming (the answer comes in pieces, we cache whole answers only)
//	yes:    temperature is exactly 0 (the model gives the same answer every time)
//	other:  a temperature above 0 or no temperature at all (the provider then uses its own
//	        default, usually 1) means the client wants a fresh, different answer each time.
//	        Only cached when allowTemperature is on.
func Cacheable(req *provider.ChatRequest, allowTemperature bool) bool {
	if req.Stream {
		return false
	}
	if allowTemperature {
		return true
	}
	return req.Temperature != nil && *req.Temperature == 0
}

// Semantic finds an answer to a question that is "almost the same" (not word for word).
// The real one is Postgres with pgvector (see semantic.go). Tests use a fake.
type Semantic interface {
	// Find looks for the stored question closest to vec, for this tenant and scope.
	// hit is true only if its similarity (1 = identical, 0 = unrelated) reaches the threshold.
	// similarity is returned in both cases, so we can log it.
	Find(ctx context.Context, tenantID, scope string, vec []float32) (entry *Entry, similarity float64, hit bool, err error)
	// Add stores a question (its text and its vector) together with the answer.
	Add(ctx context.Context, tenantID, scope, promptText string, vec []float32, entry *Entry) error
}
