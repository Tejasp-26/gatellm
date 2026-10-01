// Package embed turns text into a vector of numbers (an "embedding").
// Texts with a similar meaning get vectors that point in a similar direction.
// The semantic cache uses this to find "almost the same question".
package embed

import (
	"context"
	"math"
)

// Embedder is the one interface the cache knows.
type Embedder interface {
	// Embed returns a vector of exactly Dimensions() numbers, with length 1 (normalized).
	Embed(ctx context.Context, text string) ([]float32, error)
	Dimensions() int
}

// fit cuts the vector to size dims and scales it to length 1.
//
// Why: gemini-embedding-001 returns 3072 numbers, but our database column (and its index)
// uses 768. The model is built so that the first numbers carry the most meaning, so cutting
// is allowed. The Gemini docs say that after cutting you must normalize the vector yourself.
// With length 1, cosine similarity is simply the dot product.
func fit(v []float32, dims int) []float32 {
	if len(v) > dims {
		v = v[:dims]
	}
	out := make([]float32, len(v))
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	norm := float32(math.Sqrt(sum))
	if norm == 0 {
		return out // all zeros: leave it, the caller checks the length
	}
	for i, x := range v {
		out[i] = x / norm
	}
	return out
}
