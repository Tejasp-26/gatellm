package router

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"gatellm/internal/provider"
)

// RunHealthChecks asks every provider "are you alive?" now and then, until ctx is cancelled.
// Run it in its own goroutine.
//
// Why: without it, we only learn that a provider is down when a real client request fails,
// and only learn that it is back when a client request tests it. The ping uses the circuit
// breaker (see Resilient.Ping), so a dead provider is paused before clients notice,
// and it is let back in as soon as it answers again.
func (r *Router) RunHealthChecks(ctx context.Context, interval time.Duration) {
	r.checkAll(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.checkAll(ctx)
		}
	}
}

// checkAll pings every provider once, all at the same time.
// Targets that share a provider (for example two Groq models) cause only one ping.
func (r *Router) checkAll(ctx context.Context) {
	byProvider := map[provider.Provider][]*Target{}
	for _, t := range r.targets {
		if _, ok := t.Provider.(provider.Pinger); ok {
			byProvider[t.Provider] = append(byProvider[t.Provider], t)
		}
	}

	var wg sync.WaitGroup
	for p, targets := range byProvider {
		wg.Add(1)
		go func(p provider.Provider, targets []*Target) {
			defer wg.Done()
			pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			err := p.(provider.Pinger).Ping(pingCtx)
			record(p.Name(), targets, err)
		}(p, targets)
	}
	wg.Wait()
}

// record saves the result and logs only when the health CHANGES (no log line every 30 seconds).
func record(name string, targets []*Target, err error) {
	for _, t := range targets {
		t.mu.Lock()
		wasOK, hadCheck := t.checkOK, !t.lastCheck.IsZero()
		t.lastCheck = time.Now()
		t.checkOK = err == nil
		t.checkErr = ""
		if err != nil {
			t.checkErr = err.Error()
		}
		t.mu.Unlock()

		// Log once per provider (for its first target).
		if t != targets[0] {
			continue
		}
		switch {
		case err != nil && (wasOK || !hadCheck):
			slog.Warn("health check failed", "provider", name, "err", err)
		case err == nil && hadCheck && !wasOK:
			slog.Info("health check is OK again", "provider", name)
		}
	}
}
