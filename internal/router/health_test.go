package router

import (
	"context"
	"errors"
	"testing"
	"time"
)

// waitFor waits until the condition is true (the checks run in the background).
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestHealthChecksRunRepeatedlyAndStopWithTheContext(t *testing.T) {
	a := &fake{name: "a"}
	r := newRouter(a)

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { r.RunHealthChecks(ctx, 10*time.Millisecond); close(finished) }()

	waitFor(t, "at least 3 pings", func() bool { return a.pings.Load() >= 3 })

	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("the health check loop did not stop after the context was cancelled")
	}
	after := a.pings.Load()
	time.Sleep(50 * time.Millisecond)
	if a.pings.Load() != after {
		t.Error("pings continued after the stop")
	}
}

func TestHealthCheckResultIsSavedInTheStatus(t *testing.T) {
	good := &fake{name: "good"}
	bad := &fake{name: "bad", pingErr: errors.New("provider unreachable")}
	r := newRouter(good, bad)

	r.checkAll(context.Background())

	st := r.Status()
	if !st[0].CheckOK || st[0].CheckError != "" || st[0].LastCheck.IsZero() {
		t.Errorf("good provider: %+v", st[0])
	}
	if st[1].CheckOK || st[1].CheckError != "provider unreachable" {
		t.Errorf("bad provider: %+v", st[1])
	}
}

func TestHealthCheckRecovers(t *testing.T) {
	f := &fake{name: "f", pingErr: errors.New("down")}
	r := newRouter(f)
	r.checkAll(context.Background())
	if r.Status()[0].CheckOK {
		t.Fatal("should be unhealthy")
	}
	f.pingErr = nil
	r.checkAll(context.Background())
	if !r.Status()[0].CheckOK {
		t.Error("should be healthy again")
	}
}

func TestOneProviderWithTwoModelsIsPingedOnce(t *testing.T) {
	f := &fake{name: "groq"}
	s, _ := NewStrategy("priority")
	r := New([]*Target{
		{Provider: f, Model: "big", Weight: 1},
		{Provider: f, Model: "small", Weight: 1},
	}, s)

	r.checkAll(context.Background())
	if f.pings.Load() != 1 {
		t.Errorf("pings = %d, want 1", f.pings.Load())
	}
	st := r.Status()
	if !st[0].CheckOK || !st[1].CheckOK {
		t.Error("both targets should get the result")
	}
}

// plain has no Ping method.
type plain struct{ fake }

func (*plain) Ping() {} // a method with the same name but a different signature is NOT a Pinger

func TestProviderWithoutPingIsSkipped(t *testing.T) {
	p := &plain{fake{name: "p"}}
	s, _ := NewStrategy("priority")
	r := New([]*Target{{Provider: p, Model: "m", Weight: 1}}, s)
	r.checkAll(context.Background()) // must not crash
	if !r.Status()[0].LastCheck.IsZero() {
		t.Error("nothing was checked, so there is no result")
	}
}
