package router

import (
	"fmt"
	"sort"
	"sync"
)

// Strategy puts the targets in the order we try them.
// The FIRST one is the normal choice, the others are the fallbacks.
// The router only gives it targets that are up.
type Strategy interface {
	Order(targets []*Target) []*Target
}

// NewStrategy finds a strategy by its config name.
func NewStrategy(name string) (Strategy, error) {
	switch name {
	case "priority":
		return priority{}, nil
	case "weighted":
		return &weighted{current: map[*Target]int{}}, nil
	case "latency":
		return latency{}, nil
	}
	return nil, fmt.Errorf("ROUTE_STRATEGY must be priority, weighted or latency, got %q", name)
}

// priority: always the order from the config. The first target is the main one,
// the others are only used when it fails.
type priority struct{}

func (priority) Order(targets []*Target) []*Target {
	return append([]*Target(nil), targets...)
}

// weighted: "smooth weighted round-robin" (the same idea nginx uses).
// With weights groq=3 and gemini=1, out of every 4 requests groq gets 3 and gemini gets 1,
// and they are mixed (groq, groq, gemini, groq) instead of three in a row.
//
// How it works: every round each target adds its weight to its score.
// The highest score wins, and the winner pays back the total weight.
type weighted struct {
	mu      sync.Mutex
	current map[*Target]int // the running score of every target
}

func (w *weighted) Order(targets []*Target) []*Target {
	if len(targets) == 0 {
		return nil
	}
	w.mu.Lock()
	total := 0
	var best *Target
	for _, t := range targets {
		w.current[t] += t.Weight
		total += t.Weight
		if best == nil || w.current[t] > w.current[best] {
			best = t
		}
	}
	w.current[best] -= total
	w.mu.Unlock()

	// The winner first, the others after it in the config order (they are the fallbacks).
	ordered := []*Target{best}
	for _, t := range targets {
		if t != best {
			ordered = append(ordered, t)
		}
	}
	return ordered
}

// latency: the target with the lowest recent average goes first.
// A target without data yet counts as 0, so it gets tried and measured.
type latency struct{}

func (latency) Order(targets []*Target) []*Target {
	ordered := append([]*Target(nil), targets...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].AvgLatency() < ordered[j].AvgLatency()
	})
	return ordered
}
