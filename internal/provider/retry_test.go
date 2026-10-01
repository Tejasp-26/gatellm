package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"gatellm/internal/breaker"
)

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"429 too many requests", &ProviderError{StatusCode: 429}, true},
		{"500", &ProviderError{StatusCode: 500}, true},
		{"503", &ProviderError{StatusCode: 503}, true},
		{"400 bad request", &ProviderError{StatusCode: 400}, false},
		{"401 bad key", &ProviderError{StatusCode: 401}, false},
		{"403", &ProviderError{StatusCode: 403}, false},
		{"404", &ProviderError{StatusCode: 404}, false},
		{"timeout", context.DeadlineExceeded, true},
		{"network error", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, true},
		{"wrapped 503", fmt.Errorf("call failed: %w", &ProviderError{StatusCode: 503}), true},
		{"client left", context.Canceled, false},
		{"breaker open", breaker.ErrOpen, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsRetryable(tc.err); got != tc.want {
				t.Errorf("IsRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestBackoffDoublesAndStopsAtMax(t *testing.T) {
	cfg := RetryConfig{MaxAttempts: 10, BaseDelay: 200 * time.Millisecond, MaxDelay: 2 * time.Second}
	one := func() float64 { return 1 } // the longest wait of the jitter range

	want := []time.Duration{200, 400, 800, 1600, 2000, 2000}
	for i, w := range want {
		if got := cfg.backoff(i+1, one); got != w*time.Millisecond {
			t.Errorf("wait before retry %d = %v, want %v", i+1, got, w*time.Millisecond)
		}
	}
}

func TestBackoffJitterStaysBetweenHalfAndFull(t *testing.T) {
	cfg := RetryConfig{BaseDelay: 200 * time.Millisecond, MaxDelay: 2 * time.Second}
	zero := func() float64 { return 0 }
	if got := cfg.backoff(1, zero); got != 100*time.Millisecond {
		t.Errorf("shortest wait = %v, want 100ms", got)
	}
	// Real random numbers: always inside the range, and not always the same.
	seen := map[time.Duration]bool{}
	for i := 0; i < 100; i++ {
		d := cfg.backoff(2, newRetrier(cfg).random)
		if d < 200*time.Millisecond || d > 400*time.Millisecond {
			t.Fatalf("wait %v is outside 200ms..400ms", d)
		}
		seen[d] = true
	}
	if len(seen) < 5 {
		t.Error("the jitter should give different waits")
	}
}

// newTestRetrier records the waits instead of really waiting.
func newTestRetrier(attempts int) (*retrier, *[]time.Duration) {
	r := newRetrier(RetryConfig{MaxAttempts: attempts, BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second})
	waits := &[]time.Duration{}
	r.random = func() float64 { return 1 }
	r.sleep = func(ctx context.Context, d time.Duration) bool {
		*waits = append(*waits, d)
		return true
	}
	return r, waits
}

func TestRetrierTriesAgainUntilItWorks(t *testing.T) {
	r, waits := newTestRetrier(5)
	calls := 0
	err := r.do(context.Background(), func() error {
		calls++
		if calls < 3 {
			return &ProviderError{StatusCode: 503}
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err %v, calls %d, want success on call 3", err, calls)
	}
	if len(*waits) != 2 || (*waits)[0] != 100*time.Millisecond || (*waits)[1] != 200*time.Millisecond {
		t.Errorf("waits = %v, want [100ms 200ms]", *waits)
	}
}

func TestRetrierDoesNotRetryABadRequest(t *testing.T) {
	r, waits := newTestRetrier(5)
	calls := 0
	err := r.do(context.Background(), func() error {
		calls++
		return &ProviderError{StatusCode: 400}
	})
	if calls != 1 || len(*waits) != 0 {
		t.Errorf("a 400 must not be retried: calls %d, waits %v", calls, *waits)
	}
	var pe *ProviderError
	if !errors.As(err, &pe) || pe.StatusCode != 400 {
		t.Errorf("the original error should come back, got %v", err)
	}
}

func TestRetrierGivesUpAfterMaxAttempts(t *testing.T) {
	r, waits := newTestRetrier(3)
	calls := 0
	err := r.do(context.Background(), func() error {
		calls++
		return &ProviderError{StatusCode: 502}
	})
	if calls != 3 {
		t.Errorf("calls = %d, want exactly 3", calls)
	}
	if len(*waits) != 2 {
		t.Errorf("there is no wait after the last attempt, got waits %v", *waits)
	}
	var pe *ProviderError
	if !errors.As(err, &pe) || pe.StatusCode != 502 {
		t.Errorf("the last error should come back, got %v", err)
	}
}

func TestRetrierStopsWhenTheClientLeavesDuringTheWait(t *testing.T) {
	r, _ := newTestRetrier(5)
	r.sleep = func(ctx context.Context, d time.Duration) bool { return false } // cancelled while waiting
	calls := 0
	r.do(context.Background(), func() error {
		calls++
		return &ProviderError{StatusCode: 503}
	})
	if calls != 1 {
		t.Errorf("no new attempt after the wait was cancelled, calls = %d", calls)
	}
}

func TestRetrierWithZeroAttemptsStillTriesOnce(t *testing.T) {
	r := newRetrier(RetryConfig{MaxAttempts: 0})
	calls := 0
	r.do(context.Background(), func() error { calls++; return nil })
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

func TestRetrierCallsOnRetry(t *testing.T) {
	r, _ := newTestRetrier(3)
	var seen []int
	r.onRetry = func(attempt int, wait time.Duration, err error) { seen = append(seen, attempt) }
	r.do(context.Background(), func() error { return &ProviderError{StatusCode: 503} })
	if len(seen) != 2 || seen[0] != 1 || seen[1] != 2 {
		t.Errorf("onRetry attempts = %v, want [1 2]", seen)
	}
}
