package provider

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gatellm/internal/breaker"
)

// ResilientConfig groups the three protections.
type ResilientConfig struct {
	Timeout time.Duration // max time for ONE attempt
	Retry   RetryConfig
	Breaker breaker.Config

	// OnRetry is called before every retry (can be nil). The metrics use it.
	OnRetry func(provider string)
}

// Resilient wraps any Provider and adds a timeout, retries and a circuit breaker.
// It is a Provider itself, so the rest of the gateway does not know the difference.
//
// Order for every call:   breaker check -> attempt with timeout -> retry if it makes sense.
// Every attempt asks the breaker again, so a breaker that opens in the middle stops the retries.
type Resilient struct {
	inner   Provider
	timeout time.Duration
	retry   *retrier
	breaker *breaker.Breaker
}

func NewResilient(inner Provider, cfg ResilientConfig) *Resilient {
	name := inner.Name()

	// Log every change of the breaker, so we can see it in the logs.
	userCallback := cfg.Breaker.OnStateChange
	cfg.Breaker.OnStateChange = func(from, to breaker.State) {
		slog.Warn("circuit breaker changed state", "provider", name, "from", from.String(), "to", to.String())
		if userCallback != nil {
			userCallback(from, to)
		}
	}

	r := newRetrier(cfg.Retry)
	r.onRetry = func(attempt int, wait time.Duration, err error) {
		slog.Warn("provider call failed, will retry",
			"provider", name, "attempt", attempt, "wait_ms", wait.Milliseconds(), "err", err)
		if cfg.OnRetry != nil {
			cfg.OnRetry(name)
		}
	}
	return &Resilient{inner: inner, timeout: cfg.Timeout, retry: r, breaker: breaker.New(cfg.Breaker)}
}

func (r *Resilient) Name() string { return r.inner.Name() }

// BreakerState lets the router and the stats page see the health of this provider.
func (r *Resilient) BreakerState() breaker.State { return r.breaker.State() }

// outcomeOf tells the breaker how a call went.
func outcomeOf(err error) breaker.Outcome {
	switch {
	case err == nil:
		return breaker.Success
	case errors.Is(err, context.Canceled):
		return breaker.Ignore // the client left, this says nothing about the provider
	case IsRetryable(err):
		return breaker.Failure
	default:
		return breaker.Success // a 400 or 401: the provider answered, it is alive
	}
}

// Chat: every attempt gets its own timeout.
func (r *Resilient) Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	var resp *ChatResponse
	err := r.retry.do(ctx, func() error {
		done, err := r.breaker.Allow()
		if err != nil {
			return err
		}
		attemptCtx, cancel := context.WithTimeout(ctx, r.timeout)
		defer cancel()

		resp, err = r.inner.Chat(attemptCtx, req)
		done(outcomeOf(err))
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ChatStream retries and times out only until the stream has STARTED.
// After the first byte went to the client we cannot take it back, so a failure
// in the middle is reported to the client as it is (see the stream rules in Provider).
func (r *Resilient) ChatStream(ctx context.Context, req *ChatRequest) (<-chan StreamChunk, error) {
	var out <-chan StreamChunk
	err := r.retry.do(ctx, func() error {
		done, err := r.breaker.Allow()
		if err != nil {
			return err
		}
		ch, err := r.startStream(ctx, req, done)
		out = ch
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// startStream makes one attempt to start the stream.
// done is the breaker callback. It is called here on failure, or when the stream ends.
func (r *Resilient) startStream(ctx context.Context, req *ChatRequest, done func(breaker.Outcome)) (<-chan StreamChunk, error) {
	streamCtx, cancel := context.WithCancel(ctx)

	// The timeout only covers the start. After the stream has started the timer is stopped.
	timer := time.AfterFunc(r.timeout, cancel)
	inner, err := r.inner.ChatStream(streamCtx, req)
	timedOut := !timer.Stop() // false means the timer already fired
	if timedOut {
		err = fmt.Errorf("%w: no response within %v", context.DeadlineExceeded, r.timeout)
	}

	if err != nil {
		cancel()
		done(outcomeOf(err))
		return nil, err
	}

	// The stream started. We pass the chunks on and watch for an error in the middle,
	// because that is also a sign that the provider is unhealthy.
	out := make(chan StreamChunk)
	go func() {
		defer close(out)
		defer cancel()
		var streamErr error
		for chunk := range inner {
			if chunk.Err != nil {
				streamErr = chunk.Err
			}
			if !sendChunk(ctx, out, chunk) {
				// The client left. streamCtx is cancelled too (it is a child of ctx),
				// so the provider goroutine stops by itself.
				done(breaker.Ignore)
				return
			}
		}
		if ctx.Err() != nil {
			done(breaker.Ignore) // the client left, the stream was cut short, no verdict
			return
		}
		done(outcomeOf(streamErr))
	}()
	return out, nil
}

// Ping is a health check that goes through the circuit breaker.
//   - A failed ping counts like a failed call, so a dead provider is paused even with no traffic.
//   - After the cooldown the ping is the test call, so a provider that came back is noticed
//     and the breaker closes without waiting for a real client request.
//
// There are no retries here: the next check comes in a few seconds anyway.
func (r *Resilient) Ping(ctx context.Context) error {
	pinger, ok := r.inner.(Pinger)
	if !ok {
		return nil // this provider has no health check, nothing to do
	}
	done, err := r.breaker.Allow()
	if err != nil {
		return err
	}
	pingCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	err = pinger.Ping(pingCtx)
	done(outcomeOf(err))
	return err
}
