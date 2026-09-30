package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gatellm/internal/provider"
)

// newTestRouter builds the real router with only the mock provider.
// DB and Redis are nil, because the chat endpoint does not use them.
func newTestRouter() http.Handler {
	h := &Handler{Providers: provider.Registry{"mock": provider.NewMock(0, 0)}}
	return NewRouter(h, 1024) // 1 KB body limit, to test the size check
}

func post(body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	newTestRouter().ServeHTTP(rec, req)
	return rec
}

func TestChatSuccess(t *testing.T) {
	rec := post(`{"model":"mock","messages":[{"role":"user","content":"hi"}]}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Mock reply to: hi") {
		t.Errorf("unexpected body: %s", rec.Body.String())
	}
	if rec.Header().Get("X-Provider") != "mock" {
		t.Error("missing X-Provider header")
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("missing X-Request-ID header")
	}
}

func TestChatBadRequests(t *testing.T) {
	tests := []struct {
		name string
		body string
		want int
	}{
		{"invalid json", `{not json`, 400},
		{"missing model", `{"messages":[{"role":"user","content":"hi"}]}`, 400},
		{"empty messages", `{"model":"mock","messages":[]}`, 400},
		{"bad role", `{"model":"mock","messages":[{"role":"robot","content":"hi"}]}`, 400},
		{"unknown provider", `{"model":"foo/bar","messages":[{"role":"user","content":"hi"}]}`, 400},
		{"provider not enabled", `{"model":"groq/x","messages":[{"role":"user","content":"hi"}]}`, 400},
		{"streaming not ready", `{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}`, 501},
		{"body too large", `{"model":"mock","messages":[{"role":"user","content":"` + strings.Repeat("a", 2000) + `"}]}`, 413},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := post(tc.body)
			if rec.Code != tc.want {
				t.Errorf("got status %d, want %d, body %s", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestRecovererCatchesPanic(t *testing.T) {
	panicking := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})
	handler := RequestID(Logger(Recoverer(panicking)))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("got status %d, want 500", rec.Code)
	}
}
