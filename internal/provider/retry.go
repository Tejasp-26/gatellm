package provider

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"gatellm/internal/breaker"
)

// RetryConfig controls how often and how fast we try again.
type RetryConfig struct {
	MaxAttempts int           // total tries including the first one (3 = first try + 2 retries)
	BaseDelay   time.Duration // wait before the first retry, e.g. 200ms
	MaxDelay    time.Duration // the wait never grows above this, e.g. 2s
}

// IsRetryable says if it makes sense to try the same call again.
//
//	retry:    429 (too many requests), 5xx (provider broke), timeouts, network errors
//	no retry: 400/401/403/404 (our request is wrong, the same request fails again),
//	          client left (context.Canceled), breaker open (the breaker already decided)
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, breaker.ErrOpen) {
		return false
	}
	var pe *ProviderError
	if errors.As(err, &pe) {
		return pe.StatusCode == 429 || pe.StatusCode >= 500
	}
	return true // not an HTTP answer at all: timeout, connection refused, connection reset ...
}

// backoff is the wait before retry number n (n = 1 is the first retry).
// The wait doubles each time: 200ms, 400ms, 800ms ... up to MaxDelay.
// Then jitter: we wait a random time between half and the full value.
// Without jitter, 1000 clients that failed together would all retry together
// and hit the provider again at the same moment.
func (c RetryConfig) backoff(n int, random func() float64) time.Duration {
	d := c.BaseDelay
	for i := 1; i < n && d < c.MaxDelay; i++ {
		d *= 2
	}
	if c.MaxDelay > 0 && d > c.MaxDelay {
		d = c.MaxDelay
	}
	return d/2 + time.Duration(random()*float64(d/2))
}

// retrier runs a function again and again until it works, or we give up.
type retrier struct {
	cfg    RetryConfig
	sleep  func(ctx context.Context, d time.Duration) bool // replaced in tests
	random func() float64                                  // replaced in tests
	// onRetry is called before each wait (for logging). Can be nil.
	onRetry func(attempt int, wait time.Duration, err error)
}

func newRetrier(cfg RetryConfig) *retrier {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	return &retrier{cfg: cfg, sleep: sleepCtx, random: rand.Float64}
}

// do calls fn up to MaxAttempts times. It returns the error of the last attempt.
func (r *retrier) do(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 1; attempt <= r.cfg.MaxAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if !IsRetryable(err) || attempt == r.cfg.MaxAttempts {
			return err
		}
		wait := r.cfg.backoff(attempt, r.random)
		if r.onRetry != nil {
			r.onRetry(attempt, wait, err)
		}
		if !r.sleep(ctx, wait) {
			return err // the client left while we were waiting
		}
	}
	return err
}
