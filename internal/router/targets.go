// Package router decides WHICH provider answers a request, and what happens when one fails.
//
// A client that sends "model": "auto" is served by the router.
// The router has a list of targets (a provider plus the model to use there),
// puts them in an order (the strategy) and tries them one by one until one works (fallback).
package router

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"gatellm/internal/breaker"
	"gatellm/internal/provider"
)

// Target is one place a request can go: a provider and the model name to ask there.
type Target struct {
	Provider provider.Provider
	Model    string // the model name for THIS provider, e.g. "llama-3.1-8b-instant"
	Weight   int    // only used by the weighted strategy

	mu         sync.Mutex
	avgLatency float64 // recent average in milliseconds, 0 = no data yet
	lastCheck  time.Time
	checkOK    bool
	checkErr   string
}

// Name is the provider name, e.g. "groq".
func (t *Target) Name() string { return t.Provider.Name() }

// isUp says if the target is worth trying first.
// Only a target whose circuit breaker is OPEN is "down".
// (Half-open counts as up: one request may be the test call, and fallback protects the client.)
func (t *Target) isUp() bool {
	if b, ok := t.Provider.(interface{ BreakerState() breaker.State }); ok {
		return b.BreakerState() != breaker.Open
	}
	return true
}

// latencyAlpha decides how fast the average follows new measurements.
// 0.3 means: new average = 70% old average + 30% new measurement.
const latencyAlpha = 0.3

// observe records how long a successful call took.
func (t *Target) observe(d time.Duration) {
	ms := float64(d) / float64(time.Millisecond)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.avgLatency == 0 {
		t.avgLatency = ms // the first measurement
	} else {
		t.avgLatency = (1-latencyAlpha)*t.avgLatency + latencyAlpha*ms
	}
}

// AvgLatency is the recent average latency in milliseconds (0 = no data yet).
func (t *Target) AvgLatency() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.avgLatency
}

// DefaultTargetSpec builds ROUTE_TARGETS when the user did not set it:
// every enabled real provider, in the order groq, gemini. Without real providers it is the mock.
// The model names are examples, check the provider docs when you add real keys.
func DefaultTargetSpec(reg provider.Registry) string {
	var parts []string
	if _, ok := reg["groq"]; ok {
		parts = append(parts, "groq/llama-3.1-8b-instant")
	}
	if _, ok := reg["gemini"]; ok {
		parts = append(parts, "gemini/gemini-2.5-flash")
	}
	if len(parts) == 0 {
		return "mock"
	}
	return strings.Join(parts, ",")
}

// ParseTargets reads a text like
//
//	groq/llama-3.1-8b-instant:3,gemini/gemini-2.5-flash:1,mock
//
// Each part is  provider/model  with an optional  :weight  (default 1).
// The order of the parts is the priority order.
func ParseTargets(spec string, reg provider.Registry) ([]*Target, error) {
	var targets []*Target
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Optional weight after the last ":" (model names of Groq and Gemini have no ":").
		weight := 1
		if i := strings.LastIndex(part, ":"); i >= 0 {
			w, err := strconv.Atoi(strings.TrimSpace(part[i+1:]))
			if err != nil || w < 1 {
				return nil, fmt.Errorf("route target %q: the weight after ':' must be a whole number of 1 or more", part)
			}
			weight = w
			part = part[:i]
		}

		name, model, _ := strings.Cut(part, "/")
		p, ok := reg[name]
		if !ok {
			return nil, fmt.Errorf("route target %q: provider %q is not enabled (enabled: %s)",
				part, name, strings.Join(reg.Names(), ", "))
		}
		if model == "" && !strings.HasPrefix(name, "mock") {
			return nil, fmt.Errorf("route target %q: use the format %s/<model-name>", part, name)
		}
		targets = append(targets, &Target{Provider: p, Model: model, Weight: weight})
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("ROUTE_TARGETS has no targets")
	}
	return targets, nil
}
