package breaker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestBreaker gives a breaker with a clock that only moves when the test says so.
func newTestBreaker(threshold int, cooldown time.Duration) (*Breaker, *time.Time) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b := New(Config{FailureThreshold: threshold, Cooldown: cooldown})
	b.now = func() time.Time { return now }
	return b, &now
}

// call makes one call and reports the outcome. It returns the error from Allow.
func call(b *Breaker, outcome Outcome) error {
	done, err := b.Allow()
	if err != nil {
		return err
	}
	done(outcome)
	return nil
}

func TestOpensAfterEnoughFailuresInARow(t *testing.T) {
	b, _ := newTestBreaker(3, 10*time.Second)

	call(b, Failure)
	call(b, Failure)
	if b.State() != Closed {
		t.Fatal("2 failures are not enough")
	}
	call(b, Failure)
	if b.State() != Open {
		t.Fatalf("3 failures should open it, state is %v", b.State())
	}
	if err := call(b, Success); err != ErrOpen {
		t.Errorf("an open breaker must refuse calls, got %v", err)
	}
}

func TestASuccessResetsTheCount(t *testing.T) {
	b, _ := newTestBreaker(3, time.Second)
	call(b, Failure)
	call(b, Failure)
	call(b, Success) // the chain is broken
	call(b, Failure)
	call(b, Failure)
	if b.State() != Closed {
		t.Error("failures must be IN A ROW to open the breaker")
	}
}

func TestIgnoreDoesNotCountEitherWay(t *testing.T) {
	b, _ := newTestBreaker(2, time.Second)
	call(b, Failure)
	call(b, Ignore)
	call(b, Failure)
	if b.State() != Open {
		t.Error("Ignore should neither reset nor add to the count")
	}
}

func TestHalfOpenAfterCooldownAllowsOnlyOneProbe(t *testing.T) {
	b, now := newTestBreaker(1, 10*time.Second)
	call(b, Failure) // open

	*now = now.Add(9 * time.Second)
	if err := call(b, Success); err != ErrOpen {
		t.Fatal("still cooling down, must refuse")
	}

	*now = now.Add(2 * time.Second) // 11s: cooldown is over
	done, err := b.Allow()          // the test call
	if err != nil {
		t.Fatalf("the test call should be allowed: %v", err)
	}
	if b.State() != HalfOpen {
		t.Fatalf("state should be half-open, got %v", b.State())
	}
	if _, err := b.Allow(); err != ErrOpen {
		t.Error("only ONE call may test the provider while half-open")
	}
	done(Success)
	if b.State() != Closed {
		t.Errorf("a good test call should close the breaker, got %v", b.State())
	}
	if err := call(b, Success); err != nil {
		t.Errorf("closed again, calls must pass: %v", err)
	}
}

func TestFailedProbeOpensAgainAndRestartsCooldown(t *testing.T) {
	b, now := newTestBreaker(1, 10*time.Second)
	call(b, Failure)
	*now = now.Add(11 * time.Second)

	call(b, Failure) // the test call fails
	if b.State() != Open {
		t.Fatalf("should be open again, got %v", b.State())
	}
	*now = now.Add(5 * time.Second)
	if err := call(b, Success); err != ErrOpen {
		t.Error("the cooldown must start again from the failed test call")
	}
}

func TestIgnoredProbeLetsAnotherCallTry(t *testing.T) {
	b, now := newTestBreaker(1, time.Second)
	call(b, Failure)
	*now = now.Add(2 * time.Second)

	call(b, Ignore) // e.g. the client left during the test call
	if b.State() != HalfOpen {
		t.Fatalf("should still be half-open, got %v", b.State())
	}
	if _, err := b.Allow(); err != nil {
		t.Errorf("a new test call must be allowed: %v", err)
	}
}

// A slow call that started before the breaker opened must not close it by mistake.
func TestLateResultOfOldCallIsIgnored(t *testing.T) {
	b, now := newTestBreaker(1, time.Second)

	slowDone, _ := b.Allow() // slow call, started while closed
	call(b, Failure)         // another call fails, the breaker opens
	*now = now.Add(2 * time.Second)
	probeDone, err := b.Allow() // half-open test call
	if err != nil {
		t.Fatal(err)
	}

	slowDone(Success) // the old call finishes now
	if b.State() != HalfOpen {
		t.Fatalf("an old result must not change the state, got %v", b.State())
	}
	probeDone(Failure)
	if b.State() != Open {
		t.Errorf("the real test call failed, should be open, got %v", b.State())
	}
}

func TestOutcomeCanOnlyBeReportedOnce(t *testing.T) {
	b, _ := newTestBreaker(2, time.Second)
	done, _ := b.Allow()
	done(Failure)
	done(Failure) // a second report of the same call is ignored
	if b.State() != Closed {
		t.Error("one call must count only once")
	}
}

func TestStateChangeCallback(t *testing.T) {
	var changes []string
	b := New(Config{
		FailureThreshold: 1, Cooldown: time.Second,
		OnStateChange: func(from, to State) { changes = append(changes, from.String()+">"+to.String()) },
	})
	now := time.Now()
	b.now = func() time.Time { return now }

	call(b, Failure)
	now = now.Add(2 * time.Second)
	call(b, Success)

	want := []string{"closed>open", "open>half-open", "half-open>closed"}
	if len(changes) != 3 || changes[0] != want[0] || changes[1] != want[1] || changes[2] != want[2] {
		t.Errorf("changes = %v, want %v", changes, want)
	}
}

// The callback may use the breaker itself without a deadlock.
func TestCallbackCanCallBreaker(t *testing.T) {
	var b *Breaker
	b = New(Config{FailureThreshold: 1, Cooldown: time.Second,
		OnStateChange: func(from, to State) { _ = b.State() }})

	finished := make(chan struct{})
	go func() { call(b, Failure); close(finished) }()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock: the callback ran while the lock was held")
	}
}

// Many goroutines at once. Run with -race.
func TestConcurrentUse(t *testing.T) {
	b := New(Config{FailureThreshold: 5, Cooldown: time.Millisecond})
	var wg sync.WaitGroup
	var refused atomic.Int64
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				done, err := b.Allow()
				if err != nil {
					refused.Add(1)
					continue
				}
				if (i+j)%3 == 0 {
					done(Failure)
				} else {
					done(Success)
				}
			}
		}(i)
	}
	wg.Wait()
	_ = b.State() // we only check that nothing crashed or raced
}

func TestThresholdBelowOneIsFixed(t *testing.T) {
	b := New(Config{FailureThreshold: 0, Cooldown: time.Second})
	call(b, Failure)
	if b.State() != Open {
		t.Error("a threshold of 0 should behave like 1")
	}
}

// Once the cooldown is over, State() already says half-open (the router uses this to try the provider again).
func TestStateShowsHalfOpenAfterCooldownEvenWithoutACall(t *testing.T) {
	b, now := newTestBreaker(1, 10*time.Second)
	call(b, Failure)
	if b.State() != Open {
		t.Fatalf("state %v, want open", b.State())
	}
	*now = now.Add(11 * time.Second)
	if b.State() != HalfOpen {
		t.Errorf("state %v, want half-open after the cooldown", b.State())
	}
}
