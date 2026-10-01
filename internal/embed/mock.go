package embed

import (
	"context"
	"hash/fnv"
	"strings"
	"unicode"
)

// Mock is a fake embedder for tests and for running without an API key.
// It is a "bag of words": every word adds weight to a few places of the vector, chosen by a hash.
// Two texts with many words in common get similar vectors. It does NOT understand meaning
// (the real model knows that "car" and "automobile" are close, this one does not).
type Mock struct{ dims int }

func NewMock(dims int) *Mock { return &Mock{dims: dims} }

func (m *Mock) Dimensions() int { return m.dims }

func (m *Mock) Embed(ctx context.Context, text string) ([]float32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v := make([]float32, m.dims)
	words := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	for _, w := range words {
		h := fnv.New32a()
		h.Write([]byte(w))
		sum := h.Sum32()
		v[int(sum)%m.dims] += 1
		v[int(sum>>8)%m.dims] += 0.5 // a second place makes collisions less harmful
	}
	return fit(v, m.dims), nil
}
