package router

import (
	"context"
	"testing"
	"time"

	"gatellm/internal/breaker"
	"gatellm/internal/provider"
)

func targetsOf(weights ...int) []*Target {
	var ts []*Target
	for i, w := range weights {
		name := string(rune('a' + i))
		ts = append(ts, &Target{Provider: &fake{name: name}, Model: name, Weight: w})
	}
	return ts
}

func names(ts []*Target) string {
	s := ""
	for _, t := range ts {
		s += t.Name()
	}
	return s
}

func TestNewStrategy(t *testing.T) {
	for _, name := range []string{"priority", "weighted", "latency"} {
		if s, err := NewStrategy(name); err != nil || s == nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := NewStrategy("random"); err == nil {
		t.Error("an unknown strategy must be an error")
	}
}

func TestPriorityKeepsTheConfigOrder(t *testing.T) {
	s, _ := NewStrategy("priority")
	ts := targetsOf(1, 1, 1)
	for i := 0; i < 5; i++ {
		if got := names(s.Order(ts)); got != "abc" {
			t.Fatalf("order = %s, want abc", got)
		}
	}
}

func TestWeightedGivesTheRightShareAndMixesThem(t *testing.T) {
	s, _ := NewStrategy("weighted")
	ts := targetsOf(3, 1) // a should get 3 of every 4 requests

	first := ""
	for i := 0; i < 4; i++ {
		first += s.Order(ts)[0].Name()
	}
	if first != "aaba" {
		t.Errorf("first four picks = %s, want aaba (mixed, not aaab)", first)
	}

	count := map[string]int{}
	for i := 0; i < 400; i++ {
		count[s.Order(ts)[0].Name()]++
	}
	// 4 + 400 picks in total = 101 rounds of 4 -> a gets 303 in total, so 300 of the last 400.
	if count["a"] != 300 || count["b"] != 100 {
		t.Errorf("shares = %v, want a=300 b=100", count)
	}
}

func TestWeightedOthersAreFallbacksInConfigOrder(t *testing.T) {
	s, _ := NewStrategy("weighted")
	ts := targetsOf(1, 1, 1)
	for i := 0; i < 6; i++ {
		order := s.Order(ts)
		if len(order) != 3 {
			t.Fatalf("every target must be in the list: %s", names(order))
		}
		// after the winner, the rest keep their config order
		rest := names(order[1:])
		if rest != "bc" && rest != "ac" && rest != "ab" {
			t.Errorf("the fallbacks are not in config order: %s", names(order))
		}
	}
}

func TestWeightedSkipsDownTargetsInTheRouter(t *testing.T) {
	ts := targetsOf(5, 1)
	ts[0].Provider.(*fake).state = breaker.Open
	s, _ := NewStrategy("weighted")
	r := New(ts, s)
	for i := 0; i < 5; i++ {
		_, served, _ := r.Chat(context.Background(), &provider.ChatRequest{})
		if served.Provider != "b" {
			t.Fatalf("a is down, b must serve, got %s", served.Provider)
		}
	}
}

func TestLatencyPutsTheFastestFirst(t *testing.T) {
	ts := targetsOf(1, 1, 1)
	ts[0].observe(300 * time.Millisecond)
	ts[1].observe(100 * time.Millisecond)
	ts[2].observe(200 * time.Millisecond)

	s, _ := NewStrategy("latency")
	if got := names(s.Order(ts)); got != "bca" {
		t.Errorf("order = %s, want bca (100ms, 200ms, 300ms)", got)
	}
}

func TestLatencyTriesTargetsWithoutDataFirst(t *testing.T) {
	ts := targetsOf(1, 1)
	ts[0].observe(100 * time.Millisecond)
	s, _ := NewStrategy("latency")
	if got := names(s.Order(ts)); got != "ba" {
		t.Errorf("order = %s, want ba: b has no data and must be measured", got)
	}
}

func TestLatencyTiesKeepTheConfigOrder(t *testing.T) {
	ts := targetsOf(1, 1, 1)
	s, _ := NewStrategy("latency")
	if got := names(s.Order(ts)); got != "abc" {
		t.Errorf("order = %s, want abc", got)
	}
}

func TestLatencyAverageFollowsRecentValues(t *testing.T) {
	tg := &Target{Provider: &fake{name: "a"}}
	tg.observe(100 * time.Millisecond)
	if tg.AvgLatency() != 100 {
		t.Fatalf("the first value is taken as it is, got %v", tg.AvgLatency())
	}
	tg.observe(200 * time.Millisecond) // 0.7*100 + 0.3*200 = 130
	if got := tg.AvgLatency(); got < 129.9 || got > 130.1 {
		t.Errorf("average = %v, want 130", got)
	}
}

// The slow provider gets slower, so the router should start preferring the other one.
func TestLatencyStrategyReactsToASlowProvider(t *testing.T) {
	slow, fast := &fake{name: "slow", delay: 40 * time.Millisecond}, &fake{name: "fast"}
	s, _ := NewStrategy("latency")
	r := New([]*Target{
		{Provider: slow, Model: "m", Weight: 1},
		{Provider: fast, Model: "m", Weight: 1},
	}, s)

	var servedBy []string
	for i := 0; i < 4; i++ {
		_, served, _ := r.Chat(context.Background(), &provider.ChatRequest{})
		servedBy = append(servedBy, served.Provider)
	}
	// 1st: slow (no data, config order). 2nd: fast (no data yet, slow has data). Then fast stays first.
	if servedBy[0] != "slow" || servedBy[1] != "fast" || servedBy[2] != "fast" || servedBy[3] != "fast" {
		t.Errorf("served by %v", servedBy)
	}
}
