package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// These tests need a real Redis, because the Lua script runs inside Redis.
// Set TEST_REDIS_URL (for example redis://localhost:6379) to run them.
// Without it they are skipped.

// fakeClock lets a test move time forward without sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newTestLimiter gives a Limiter, a fake clock and a tenant id that is unique to the test.
func newTestLimiter(t *testing.T, failOpen bool) (*Limiter, *fakeClock, string) {
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

	clock := &fakeClock{t: time.Now()}
	l := New(rdb, failOpen, time.Second)
	l.now = clock.Now

	tenant := fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())
	t.Cleanup(func() {
		rdb.Del(context.Background(), requestsKey(tenant), tokensKey(tenant))
	})
	return l, clock, tenant
}

func mustAllow(t *testing.T, l *Limiter, tenant string, lim Limits, cost int64) Decision {
	t.Helper()
	d, err := l.Allow(context.Background(), tenant, lim, cost)
	if err != nil {
		t.Fatalf("Allow returned an error: %v", err)
	}
	return d
}

func TestRequestsPerMinute(t *testing.T) {
	l, _, tenant := newTestLimiter(t, false)
	lim := Limits{RPM: 3, TPM: 100000}

	for i := 1; i <= 3; i++ {
		if d := mustAllow(t, l, tenant, lim, 1); !d.Allowed {
			t.Fatalf("request %d should pass: %+v", i, d)
		}
	}
	d := mustAllow(t, l, tenant, lim, 1)
	if d.Allowed || d.Reason != "rpm" {
		t.Fatalf("4th request should be denied by rpm: %+v", d)
	}
	// 3 per minute = one token every 20 seconds.
	if d.RetryAfter < 19*time.Second || d.RetryAfter > 21*time.Second {
		t.Errorf("RetryAfter should be about 20s, got %v", d.RetryAfter)
	}
}

func TestTokensPerMinute(t *testing.T) {
	l, _, tenant := newTestLimiter(t, false)
	lim := Limits{RPM: 1000, TPM: 1000}

	if d := mustAllow(t, l, tenant, lim, 600); !d.Allowed || d.RemainingTokens != 400 {
		t.Fatalf("first call should pass with 400 left: %+v", d)
	}
	d := mustAllow(t, l, tenant, lim, 600)
	if d.Allowed || d.Reason != "tpm" {
		t.Fatalf("second call should be denied by tpm: %+v", d)
	}
	// We miss 200 tokens; the bucket makes 1000 per 60s, so about 12 seconds.
	if d.RetryAfter < 11*time.Second || d.RetryAfter > 13*time.Second {
		t.Errorf("RetryAfter should be about 12s, got %v", d.RetryAfter)
	}
}

// If the token check fails, the request counter must NOT go down.
func TestDeniedRequestTakesNothing(t *testing.T) {
	l, _, tenant := newTestLimiter(t, false)
	lim := Limits{RPM: 2, TPM: 100}

	// Too many tokens: denied. Try it many times.
	for i := 0; i < 10; i++ {
		if d := mustAllow(t, l, tenant, lim, 500); d.Allowed {
			t.Fatal("should be denied")
		}
	}
	// Both request slots must still be there.
	for i := 1; i <= 2; i++ {
		if d := mustAllow(t, l, tenant, lim, 10); !d.Allowed {
			t.Fatalf("request %d should pass, denied requests must not use the quota: %+v", i, d)
		}
	}
}

func TestBucketRefillsOverTime(t *testing.T) {
	l, clock, tenant := newTestLimiter(t, false)
	lim := Limits{RPM: 60, TPM: 100000} // one request per second

	for i := 0; i < 60; i++ {
		mustAllow(t, l, tenant, lim, 1)
	}
	if d := mustAllow(t, l, tenant, lim, 1); d.Allowed {
		t.Fatal("bucket should be empty")
	}

	clock.Advance(3 * time.Second) // 3 tokens come back
	for i := 1; i <= 3; i++ {
		if d := mustAllow(t, l, tenant, lim, 1); !d.Allowed {
			t.Fatalf("request %d after waiting should pass: %+v", i, d)
		}
	}
	if d := mustAllow(t, l, tenant, lim, 1); d.Allowed {
		t.Fatal("only 3 tokens should have come back")
	}

	clock.Advance(10 * time.Minute) // the bucket never holds more than its capacity
	d := mustAllow(t, l, tenant, lim, 1)
	if !d.Allowed || d.RemainingRequests != 59 {
		t.Errorf("bucket should be full (59 left after one call): %+v", d)
	}
}

func TestTenantsAreSeparate(t *testing.T) {
	l, _, tenantA := newTestLimiter(t, false)
	tenantB := tenantA + "-b"
	t.Cleanup(func() { l.rdb.Del(context.Background(), requestsKey(tenantB), tokensKey(tenantB)) })
	lim := Limits{RPM: 1, TPM: 1000}

	mustAllow(t, l, tenantA, lim, 1)
	if d := mustAllow(t, l, tenantA, lim, 1); d.Allowed {
		t.Fatal("tenant A should be limited")
	}
	if d := mustAllow(t, l, tenantB, lim, 1); !d.Allowed {
		t.Fatal("tenant B must not be affected by tenant A")
	}
}

func TestAdjustGivesTokensBackAndCanCreateDebt(t *testing.T) {
	l, _, tenant := newTestLimiter(t, false)
	lim := Limits{RPM: 1000, TPM: 1000}
	ctx := context.Background()

	mustAllow(t, l, tenant, lim, 800) // 200 left

	// We estimated 800 but the real usage was 300: give 500 back.
	if err := l.Adjust(ctx, tenant, lim, -500); err != nil {
		t.Fatal(err)
	}
	if d := mustAllow(t, l, tenant, lim, 650); !d.Allowed {
		t.Fatalf("after the refund 700 tokens should be free: %+v", d)
	}

	// Now the real usage was bigger than the estimate: the bucket goes below zero.
	if err := l.Adjust(ctx, tenant, lim, 400); err != nil {
		t.Fatal(err)
	}
	d := mustAllow(t, l, tenant, lim, 1)
	if d.Allowed || d.Reason != "tpm" {
		t.Fatalf("the tenant owes tokens, so it must be denied: %+v", d)
	}
}

func TestRefundNeverGoesOverCapacity(t *testing.T) {
	l, _, tenant := newTestLimiter(t, false)
	lim := Limits{RPM: 1000, TPM: 1000}

	mustAllow(t, l, tenant, lim, 100)
	l.Adjust(context.Background(), tenant, lim, -5000) // a huge refund

	d := mustAllow(t, l, tenant, lim, 1)
	if d.RemainingTokens > 999 {
		t.Errorf("bucket went over its capacity: %+v", d)
	}
}

func TestAdjustDoesNothingWithoutBucket(t *testing.T) {
	l, _, tenant := newTestLimiter(t, false)
	lim := Limits{RPM: 10, TPM: 1000}

	if err := l.Adjust(context.Background(), tenant, lim, 500); err != nil {
		t.Fatal(err)
	}
	exists, _ := l.rdb.Exists(context.Background(), tokensKey(tenant)).Result()
	if exists != 0 {
		t.Error("Adjust must not create a bucket that does not exist")
	}
}

// The most important test: many requests at the same time must not get past the limit.
// Run it with -race.
func TestConcurrentRequestsRespectTheLimit(t *testing.T) {
	l, _, tenant := newTestLimiter(t, false)
	lim := Limits{RPM: 50, TPM: 1000000}

	var allowed, denied atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := l.Allow(context.Background(), tenant, lim, 1)
			if err != nil {
				t.Error(err)
				return
			}
			if d.Allowed {
				allowed.Add(1)
			} else {
				denied.Add(1)
			}
		}()
	}
	wg.Wait()

	if allowed.Load() != 50 || denied.Load() != 50 {
		t.Errorf("expected exactly 50 allowed and 50 denied, got %d and %d", allowed.Load(), denied.Load())
	}
}

func TestLimitZeroDeniesEverything(t *testing.T) {
	l, _, tenant := newTestLimiter(t, false)
	d, err := l.Allow(context.Background(), tenant, Limits{RPM: 0, TPM: 100}, 1)
	if err != nil || d.Allowed {
		t.Errorf("a limit of 0 should deny: %+v, %v", d, err)
	}
}

// ---- Redis is down ----
// These two do not need a real Redis: we point the client to a port where nothing listens.

func deadRedis() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: 0, DialTimeout: 200 * time.Millisecond})
}

func TestRedisDownFailClosed(t *testing.T) {
	l := New(deadRedis(), false, 300*time.Millisecond)
	d, err := l.Allow(context.Background(), "t1", Limits{RPM: 10, TPM: 100}, 1)
	if !errors.Is(err, ErrUnavailable) || d.Allowed {
		t.Errorf("fail-closed must return ErrUnavailable, got %+v, %v", d, err)
	}
}

func TestRedisDownFailOpen(t *testing.T) {
	l := New(deadRedis(), true, 300*time.Millisecond)
	d, err := l.Allow(context.Background(), "t1", Limits{RPM: 10, TPM: 100}, 1)
	if err != nil || !d.Allowed || !d.Degraded {
		t.Errorf("fail-open must allow and mark the answer as degraded, got %+v, %v", d, err)
	}
}

func TestRetryAfterSeconds(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want int
	}{
		{0, 1}, {1 * time.Millisecond, 1}, {1000 * time.Millisecond, 1}, {1001 * time.Millisecond, 2}, {20 * time.Second, 20},
	}
	for _, tc := range tests {
		if got := RetryAfterSeconds(tc.in); got != tc.want {
			t.Errorf("RetryAfterSeconds(%v) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
