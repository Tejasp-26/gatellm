package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// post sends a chat request with a valid API key.
func post(body string) *httptest.ResponseRecorder {
	srv, _ := newTestServer(testAdminToken)
	return do(srv, http.MethodPost, "/v1/chat/completions", body, "Bearer "+testAPIKey)
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
