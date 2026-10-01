// Package breaker is a small circuit breaker, written by hand.
//
// Idea: when a provider keeps failing, stop calling it for a while.
// This saves time (no waiting for timeouts) and gives the provider room to recover.
//
//	CLOSED    normal. Calls go through. Failures are counted.
//	OPEN      too many failures in a row. Calls fail at once, nothing is sent.
//	HALF-OPEN after the cooldown. ONE test call is allowed.
//	          If it works the breaker closes, if it fails it opens again.
package breaker

import (
	"errors"
	"sync"
	"time"
)

// State is the state of the breaker.
type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	default:
		return "half-open"
	}
}

// ErrOpen is returned by Allow when the call is not allowed.
var ErrOpen = errors.New("circuit breaker is open")

// Outcome is how a call ended. The caller reports it to the breaker.
type Outcome int

const (
	Success Outcome = iota // the provider answered (an error like 400 also counts: it is alive)
	Failure                // timeout, network error, 5xx, 429
	Ignore                 // unknown result, for example the client left. Says nothing about the provider.
)

// Config holds the settings.
type Config struct {
	FailureThreshold int           // how many failures in a row open the breaker
	Cooldown         time.Duration // how long it stays open before the test call
	// OnStateChange is called after every change (can be nil). Use it for logs and metrics.
	OnStateChange func(from, to State)
}

// Breaker is safe to use from many goroutines.
type Breaker struct {
	cfg Config
	now func() time.Time // replaced in tests

	mu         sync.Mutex
	state      State
	failures   int       // failures in a row (only used when closed)
	openedAt   time.Time // when it opened
	probing    bool      // true while the half-open test call is running
	generation uint64    // grows with every state change, see Allow
}

// New creates a closed breaker.
func New(cfg Config) *Breaker {
	if cfg.FailureThreshold < 1 {
		cfg.FailureThreshold = 1
	}
	return &Breaker{cfg: cfg, now: time.Now}
}

// State returns the current state.
// An open breaker whose cooldown is over is reported as half-open: the next call will be the test call.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == Open && b.now().Sub(b.openedAt) >= b.cfg.Cooldown {
		return HalfOpen
	}
	return b.state
}

// Allow asks: may I make a call now?
// If yes, it returns a function. Call it ONCE with the outcome when the call is finished.
// If not, it returns ErrOpen.
func (b *Breaker) Allow() (func(Outcome), error) {
	b.mu.Lock()

	var from, to State
	changed := false

	// An open breaker becomes half-open when the cooldown is over.
	if b.state == Open && b.now().Sub(b.openedAt) >= b.cfg.Cooldown {
		from, to = b.setState(HalfOpen), HalfOpen
		changed = true
	}

	switch {
	case b.state == Open, b.state == HalfOpen && b.probing:
		b.mu.Unlock()
		b.notify(changed, from, to)
		return nil, ErrOpen
	case b.state == HalfOpen:
		b.probing = true // this call is the test call
	}

	// The result of a call only counts if the breaker is still in the same "generation".
	// Example: a slow call started while closed, and the breaker opened meanwhile.
	// Its late result must not be mistaken for the result of the half-open test call.
	gen := b.generation
	b.mu.Unlock()
	b.notify(changed, from, to)

	used := false
	return func(outcome Outcome) { b.finish(gen, &used, outcome) }, nil
}

// finish records the outcome of one call.
func (b *Breaker) finish(gen uint64, used *bool, outcome Outcome) {
	b.mu.Lock()
	if *used || gen != b.generation {
		b.mu.Unlock() // reported twice, or the state changed since the call started
		return
	}
	*used = true

	var from, to State
	changed := false

	switch b.state {
	case Closed:
		switch outcome {
		case Success:
			b.failures = 0
		case Failure:
			b.failures++
			if b.failures >= b.cfg.FailureThreshold {
				from, to = b.setState(Open), Open
				changed = true
			}
		}
	case HalfOpen:
		switch outcome {
		case Success:
			from, to = b.setState(Closed), Closed
			changed = true
		case Failure:
			from, to = b.setState(Open), Open
			changed = true
		case Ignore:
			b.probing = false // the test call said nothing, so another call may try
		}
	}
	b.mu.Unlock()
	b.notify(changed, from, to)
}

// setState changes the state (the caller holds the lock) and returns the old state.
func (b *Breaker) setState(to State) State {
	from := b.state
	b.state = to
	b.generation++
	b.failures = 0
	b.probing = false
	if to == Open {
		b.openedAt = b.now()
	}
	return from
}

// notify calls the callback AFTER the lock was released, so the callback can safely use the breaker.
func (b *Breaker) notify(changed bool, from, to State) {
	if changed && b.cfg.OnStateChange != nil {
		b.cfg.OnStateChange(from, to)
	}
}
