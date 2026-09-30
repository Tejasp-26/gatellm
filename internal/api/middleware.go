package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
)

// ctxKey is our own type, so our context key can never clash with another package's key.
type ctxKey string

const requestIDKey ctxKey = "request_id"

// RequestIDFrom gets the request id out of the context (empty string if missing).
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// RequestID gives every request a unique id, returns it in the X-Request-ID
// header and stores it in the context, so logs of one request can be found together.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, 8)
		rand.Read(b)
		id := hex.EncodeToString(b)

		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// statusRecorder remembers the status code, so the logger can print it.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Flush is needed for streaming (Phase 3), so we pass it through now.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Logger writes one log line per request.
// We log only method, path, status and time. Never the body, prompts or API keys.
func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		slog.Info("request",
			"request_id", RequestIDFrom(r.Context()),
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

// Recoverer catches a panic in any handler, so one bad request cannot crash the whole server.
func Recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if rec == http.ErrAbortHandler {
					panic(rec) // this one is a deliberate abort, let net/http handle it
				}
				slog.Error("panic recovered",
					"request_id", RequestIDFrom(r.Context()),
					"panic", rec,
					"stack", string(debug.Stack()),
				)
				writeError(w, http.StatusInternalServerError, "server_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// BodyLimit stops clients from sending huge request bodies.
func BodyLimit(maxBytes int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			next.ServeHTTP(w, r)
		})
	}
}
