package cache

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"gatellm/internal/provider"
)

func f64(v float64) *float64 { return &v }

func baseReq() *provider.ChatRequest {
	return &provider.ChatRequest{
		Model:       "ignored-here",
		Messages:    []provider.Message{{Role: "system", Content: "be short"}, {Role: "user", Content: "hello"}},
		Temperature: f64(0),
		MaxTokens:   50,
	}
}

// ---- the key ----

func TestSameInputGivesSameKey(t *testing.T) {
	a := Key("t1", "mock", baseReq())
	b := Key("t1", "mock", baseReq())
	if a != b {
		t.Errorf("same input must give the same key: %s vs %s", a, b)
	}
	if !strings.HasPrefix(a, "cache:t1:") {
		t.Errorf("key should start with cache:<tenant>: but is %s", a)
	}
}

func TestKeyChangesWhenAnythingThatMattersChanges(t *testing.T) {
	base := Key("t1", "mock", baseReq())

	changes := map[string]func() string{
		"tenant":  func() string { return Key("t2", "mock", baseReq()) },
		"model":   func() string { return Key("t1", "groq/x", baseReq()) },
		"auto":    func() string { return Key("t1", "auto", baseReq()) },
		"content": func() string { r := baseReq(); r.Messages[1].Content = "hello!"; return Key("t1", "mock", r) },
		"role":    func() string { r := baseReq(); r.Messages[0].Role = "user"; return Key("t1", "mock", r) },
		"order": func() string {
			r := baseReq()
			r.Messages[0], r.Messages[1] = r.Messages[1], r.Messages[0]
			return Key("t1", "mock", r)
		},
		"extra message": func() string {
			r := baseReq()
			r.Messages = append(r.Messages, provider.Message{Role: "user", Content: "more"})
			return Key("t1", "mock", r)
		},
		"temperature": func() string { r := baseReq(); r.Temperature = f64(0.7); return Key("t1", "mock", r) },
		"no temp":     func() string { r := baseReq(); r.Temperature = nil; return Key("t1", "mock", r) },
		"max_tokens":  func() string { r := baseReq(); r.MaxTokens = 51; return Key("t1", "mock", r) },
	}
	seen := map[string]string{base: "base"}
	for name, fn := range changes {
		k := fn()
		if k == base {
			t.Errorf("changing %s must change the key", name)
		}
		if other, dup := seen[k]; dup {
			t.Errorf("%s and %s gave the same key", name, other)
		}
		seen[k] = name
	}
}

func TestKeyIgnoresStreamSettings(t *testing.T) {
	r := baseReq()
	r.Stream = true
	r.StreamOptions = &provider.StreamOptions{IncludeUsage: true}
	if Key("t1", "mock", r) != Key("t1", "mock", baseReq()) {
		t.Error("stream settings change how the answer is delivered, not the answer")
	}
}

// ---- which requests may be cached ----

func TestCacheable(t *testing.T) {
	tests := []struct {
		name  string
		mod   func(r *provider.ChatRequest)
		allow bool
		want  bool
	}{
		{"temperature 0", func(r *provider.ChatRequest) {}, false, true},
		{"temperature 0.7", func(r *provider.ChatRequest) { r.Temperature = f64(0.7) }, false, false},
		{"no temperature", func(r *provider.ChatRequest) { r.Temperature = nil }, false, false},
		{"stream", func(r *provider.ChatRequest) { r.Stream = true }, false, false},
		{"temperature 0.7 but flag on", func(r *provider.ChatRequest) { r.Temperature = f64(0.7) }, true, true},
		{"no temperature but flag on", func(r *provider.ChatRequest) { r.Temperature = nil }, true, true},
		{"stream even with flag on", func(r *provider.ChatRequest) { r.Stream = true }, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := baseReq()
			tc.mod(r)
			if got := Cacheable(r, tc.allow); got != tc.want {
				t.Errorf("Cacheable = %v, want %v", got, tc.want)
			}
		})
	}
}

// ---- Redis (set TEST_REDIS_URL to run) ----

func newTestRedis(t *testing.T, ttl time.Duration) (*Redis, *redis.Client) {
	t.Helper()
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("TEST_REDIS_URL is not set, skipping the Redis tests")
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis is not reachable: %v", err)
	}
	return NewRedis(rdb, ttl, time.Second), rdb
}

func testEntry(text string) *Entry {
	return &Entry{
		Provider: "mock", Model: "m1",
		Response: &provider.ChatResponse{ID: "x", Choices: []provider.Choice{{Message: provider.Message{Role: "assistant", Content: text}}}},
	}
}

func TestRedisRoundTripAndMiss(t *testing.T) {
	c, rdb := newTestRedis(t, time.Minute)
	ctx := context.Background()
	key := Key("test-"+t.Name(), "mock", baseReq())
	t.Cleanup(func() { rdb.Del(ctx, key) })

	if _, hit, err := c.Get(ctx, key); hit || err != nil {
		t.Fatalf("empty cache must be a clean miss: hit=%v err=%v", hit, err)
	}
	if err := c.Set(ctx, key, testEntry("hi there")); err != nil {
		t.Fatal(err)
	}
	e, hit, err := c.Get(ctx, key)
	if err != nil || !hit {
		t.Fatalf("expected a hit: hit=%v err=%v", hit, err)
	}
	if e.Provider != "mock" || e.Model != "m1" || e.Response.Choices[0].Message.Content != "hi there" {
		t.Errorf("wrong entry back: %+v", e)
	}
}

func TestRedisEntryExpires(t *testing.T) {
	c, rdb := newTestRedis(t, 1500*time.Millisecond)
	ctx := context.Background()
	key := Key("test-"+t.Name(), "mock", baseReq())
	t.Cleanup(func() { rdb.Del(ctx, key) })

	c.Set(ctx, key, testEntry("short life"))
	if ttl := rdb.PTTL(ctx, key).Val(); ttl <= 0 || ttl > 1500*time.Millisecond {
		t.Errorf("the entry must have the TTL, got %v", ttl)
	}
	time.Sleep(1700 * time.Millisecond)
	if _, hit, _ := c.Get(ctx, key); hit {
		t.Error("the entry should be gone after the TTL")
	}
}

func TestRedisDamagedEntryIsAnErrorNotAHit(t *testing.T) {
	c, rdb := newTestRedis(t, time.Minute)
	ctx := context.Background()
	key := Key("test-"+t.Name(), "mock", baseReq())
	t.Cleanup(func() { rdb.Del(ctx, key) })

	rdb.Set(ctx, key, "{not json", time.Minute)
	if _, hit, err := c.Get(ctx, key); hit || err == nil {
		t.Errorf("a damaged entry must not be served: hit=%v err=%v", hit, err)
	}
	rdb.Set(ctx, key, `{"provider":"mock"}`, time.Minute) // valid JSON but no response
	if _, hit, err := c.Get(ctx, key); hit || err == nil {
		t.Errorf("an entry without a response must not be served: hit=%v err=%v", hit, err)
	}
}

func TestRedisHugeAnswerIsNotStored(t *testing.T) {
	c, rdb := newTestRedis(t, time.Minute)
	ctx := context.Background()
	key := Key("test-"+t.Name(), "mock", baseReq())
	t.Cleanup(func() { rdb.Del(ctx, key) })

	if err := c.Set(ctx, key, testEntry(strings.Repeat("a", maxEntryBytes+1))); err != nil {
		t.Fatalf("too big is not an error, we just skip it: %v", err)
	}
	if _, hit, _ := c.Get(ctx, key); hit {
		t.Error("an answer over the size limit must not be stored")
	}
}

func TestRedisTenantsDoNotShareAnswers(t *testing.T) {
	c, rdb := newTestRedis(t, time.Minute)
	ctx := context.Background()
	a := Key("test-A-"+t.Name(), "mock", baseReq())
	b := Key("test-B-"+t.Name(), "mock", baseReq())
	t.Cleanup(func() { rdb.Del(ctx, a, b) })

	c.Set(ctx, a, testEntry("secret of A"))
	if _, hit, _ := c.Get(ctx, b); hit {
		t.Error("tenant B must never get the answer stored for tenant A")
	}
}

func TestRedisDownGivesAnError(t *testing.T) {
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: 0, DialTimeout: 100 * time.Millisecond})
	c := NewRedis(dead, time.Minute, 300*time.Millisecond)
	if _, _, err := c.Get(context.Background(), "k"); err == nil {
		t.Error("Get with Redis down must return an error (the caller treats it as a miss)")
	}
	if err := c.Set(context.Background(), "k", testEntry("x")); err == nil {
		t.Error("Set with Redis down must return an error")
	}
}
