package embed

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func dot(a, b []float32) float64 {
	var s float64
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}

func length(v []float32) float64 { return math.Sqrt(dot(v, v)) }

func TestFitCutsAndNormalizes(t *testing.T) {
	v := fit([]float32{3, 4, 100, 100}, 2) // keeps 3 and 4
	if len(v) != 2 {
		t.Fatalf("length %d, want 2", len(v))
	}
	if math.Abs(float64(v[0])-0.6) > 1e-6 || math.Abs(float64(v[1])-0.8) > 1e-6 {
		t.Errorf("want [0.6 0.8], got %v", v)
	}
}

func TestFitKeepsAllZeroVector(t *testing.T) {
	v := fit([]float32{0, 0, 0}, 3)
	if len(v) != 3 || v[0] != 0 {
		t.Errorf("a zero vector stays zero, got %v", v)
	}
}

func TestMockIsNormalizedAndStable(t *testing.T) {
	m := NewMock(64)
	a, _ := m.Embed(context.Background(), "What is the capital of France?")
	b, _ := m.Embed(context.Background(), "What is the capital of France?")
	if len(a) != 64 || math.Abs(length(a)-1) > 1e-5 {
		t.Errorf("want 64 numbers of length 1, got %d and %v", len(a), length(a))
	}
	if dot(a, b) < 0.9999 {
		t.Error("the same text must give the same vector")
	}
}

func TestMockSimilarTextsAreCloserThanDifferentOnes(t *testing.T) {
	m := NewMock(768)
	e := func(s string) []float32 { v, _ := m.Embed(context.Background(), s); return v }
	base := e("what is the capital of france")
	near := e("What is the capital of France?") // only case and punctuation differ
	close := e("tell me the capital of france")
	far := e("how do I bake sourdough bread at home")

	if dot(base, near) < 0.999 {
		t.Errorf("case and punctuation must not matter, got %v", dot(base, near))
	}
	if !(dot(base, close) > dot(base, far)+0.3) {
		t.Errorf("shared words must be closer: close %v, far %v", dot(base, close), dot(base, far))
	}
}

func TestMockStopsWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewMock(8).Embed(ctx, "x"); err == nil {
		t.Error("a cancelled context must give an error")
	}
}

// A fake Gemini server: checks the request format and answers with 3072 numbers.
func fakeGemini(t *testing.T, status int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/embeddings" || r.Method != http.MethodPost {
			t.Errorf("wrong request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer secret-key" {
			t.Errorf("wrong auth header %q", r.Header.Get("Authorization"))
		}
		var in map[string]any
		json.NewDecoder(r.Body).Decode(&in)
		if in["model"] != "gemini-embedding-001" || in["input"] != "hello" {
			t.Errorf("wrong body: %v", in)
		}
		if status != 200 {
			w.WriteHeader(status)
			w.Write([]byte(`{"error":"hello is my secret prompt"}`))
			return
		}
		vec := make([]float32, 3072)
		for i := range vec {
			vec[i] = float32(i%7 + 1)
		}
		json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"embedding": vec}}})
	}))
}

func TestGeminiEmbedCutsTo768AndNormalizes(t *testing.T) {
	srv := fakeGemini(t, 200)
	defer srv.Close()
	e := NewOpenAICompatible(srv.URL, "secret-key", "gemini-embedding-001", 768, 2*time.Second)

	v, err := e.Embed(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 768 || math.Abs(length(v)-1) > 1e-5 {
		t.Errorf("want 768 numbers of length 1, got %d and %v", len(v), length(v))
	}
}

func TestGeminiErrorDoesNotLeakTheBody(t *testing.T) {
	srv := fakeGemini(t, 429)
	defer srv.Close()
	e := NewOpenAICompatible(srv.URL, "secret-key", "gemini-embedding-001", 768, 2*time.Second)

	_, err := e.Embed(context.Background(), "hello")
	if err == nil || !strings.Contains(err.Error(), "429") {
		t.Fatalf("want an error with the status, got %v", err)
	}
	if strings.Contains(err.Error(), "secret prompt") {
		t.Error("the response body (which may repeat the prompt) must not be in the error")
	}
}

func TestGeminiTooShortVectorIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data":[{"embedding":[0.1,0.2]}]}`))
	}))
	defer srv.Close()
	e := NewOpenAICompatible(srv.URL, "k", "m", 768, time.Second)
	if _, err := e.Embed(context.Background(), "x"); err == nil {
		t.Error("a vector shorter than expected must be an error, not stored")
	}
}

func TestGeminiBadJSONIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`nope`)) }))
	defer srv.Close()
	e := NewOpenAICompatible(srv.URL, "k", "m", 8, time.Second)
	if _, err := e.Embed(context.Background(), "x"); err == nil {
		t.Error("expected an error")
	}
}
