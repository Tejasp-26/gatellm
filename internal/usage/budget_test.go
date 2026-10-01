package usage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ---- pricing (no Redis needed) ----

func TestCostUSD(t *testing.T) {
	// gpt-oss-120b: $0.15 input and $0.60 output per 1M tokens.
	got := CostUSD("groq", "openai/gpt-oss-120b", 1_000_000, 1_000_000)
	if math.Abs(got-0.75) > 1e-9 {
		t.Errorf("cost = %v, want 0.75", got)
	}
	// 1000 input tokens only.
	got = CostUSD("groq", "openai/gpt-oss-120b", 1000, 0)
	if math.Abs(got-0.00015) > 1e-12 {
		t.Errorf("cost = %v, want 0.00015", got)
	}
}

func TestUnknownModelUsesDefaultPriceNotFree(t *testing.T) {
	if CostUSD("gemini", "some-new-model", 1000, 1000) <= 0 {
		t.Error("an unknown model must not be free")
	}
	if PriceFor("gemini", "some-new-model") != defaultPrice {
		t.Error("unknown model should use the default price")
	}
}

func TestMockHasOnePrice(t *testing.T) {
	if CostUSD("mock", "", 100, 100) <= 0 {
		t.Error("the mock needs a (fake) price so the budget can be tested")
	}
	if PriceFor("mock", "anything") != PriceFor("mock", "") {
		t.Error("all mock models should share one price")
	}
}

func TestBudgetKeyChangesEveryMonth(t *testing.T) {
	oct := budgetKey("t1", time.Date(2026, 10, 31, 23, 59, 0, 0, time.UTC))
	nov := budgetKey("t1", time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	if oct == nov {
		t.Error("a new month must use a new key")
	}
	if oct != "budget:{t1}:2026-10" {
		t.Errorf("unexpected key %q", oct)
	}
	// Same moment in another time zone is still the same UTC month.
	ist := time.FixedZone("IST", 5*3600+1800)
	if budgetKey("t1", time.Date(2026, 11, 1, 3, 0, 0, 0, ist)) != oct {
		t.Error("the month must be taken in UTC (1 Nov 03:00 IST is still 31 Oct in UTC)")
	}
}

// ---- Redis tests: set TEST_REDIS_URL to run them ----

func newTestBudget(t *testing.T, failOpen bool) (*Budget, string) {
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

	clock := time.Now().UTC() // the real time, because Redis expiry uses real time
	b := NewBudget(rdb, failOpen, time.Second)
	b.now = func() time.Time { return clock }

	tenant := fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())
	t.Cleanup(func() {
		rdb.Del(context.Background(), budgetKey(tenant, clock))
		rdb.Del(context.Background(), budgetKey(tenant, clock.AddDate(0, 1, 0)))
	})
	return b, tenant
}

func TestBudgetAllowsUntilSpent(t *testing.T) {
	b, tenant := newTestBudget(t, false)
	ctx := context.Background()

	st, err := b.Check(ctx, tenant, 1.00)
	if err != nil || !st.Allowed || st.SpentUSD != 0 || st.RemainingUSD != 1.00 {
		t.Fatalf("fresh tenant should be allowed: %+v, %v", st, err)
	}

	b.Add(ctx, tenant, 0.40)
	b.Add(ctx, tenant, 0.35)
	st, _ = b.Check(ctx, tenant, 1.00)
	if !st.Allowed || math.Abs(st.SpentUSD-0.75) > 1e-9 || math.Abs(st.RemainingUSD-0.25) > 1e-9 {
		t.Fatalf("0.75 spent, 0.25 left: %+v", st)
	}

	b.Add(ctx, tenant, 0.25) // exactly the whole budget
	st, _ = b.Check(ctx, tenant, 1.00)
	if st.Allowed || st.RemainingUSD != 0 {
		t.Fatalf("budget is used up, must be denied: %+v", st)
	}
}

func TestBudgetZeroAllowsNothing(t *testing.T) {
	b, tenant := newTestBudget(t, false)
	st, err := b.Check(context.Background(), tenant, 0)
	if err != nil || st.Allowed {
		t.Errorf("a budget of 0 must deny: %+v, %v", st, err)
	}
}

func TestBudgetResetsInANewMonth(t *testing.T) {
	b, tenant := newTestBudget(t, false)
	ctx := context.Background()
	thisMonth := b.now()
	nextMonth := time.Date(thisMonth.Year(), thisMonth.Month()+1, 1, 0, 0, 0, 0, time.UTC)

	b.Add(ctx, tenant, 5)
	if st, _ := b.Check(ctx, tenant, 5); st.Allowed {
		t.Fatal("this month's budget is used up")
	}

	b.now = func() time.Time { return nextMonth }
	if st, _ := b.Check(ctx, tenant, 5); !st.Allowed || st.SpentUSD != 0 {
		t.Fatalf("next month must start at zero: %+v", st)
	}
}

func TestBudgetKeyExpires(t *testing.T) {
	b, tenant := newTestBudget(t, false)
	b.Add(context.Background(), tenant, 1)

	ttl, err := b.rdb.TTL(context.Background(), budgetKey(tenant, b.now())).Result()
	if err != nil || ttl <= 0 {
		t.Fatalf("the counter must have an expiry, got %v, %v", ttl, err)
	}
	// The key should disappear 7 days after the month ends.
	now := b.now()
	nextMonth := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
	want := time.Until(nextMonth.Add(7 * 24 * time.Hour))
	if diff := ttl - want; diff > time.Minute || diff < -time.Minute {
		t.Errorf("expiry is wrong: ttl %v, want about %v", ttl, want)
	}
}

func TestBudgetTenantsAreSeparate(t *testing.T) {
	b, tenantA := newTestBudget(t, false)
	tenantB := tenantA + "-b"
	t.Cleanup(func() { b.rdb.Del(context.Background(), budgetKey(tenantB, b.now())) })
	ctx := context.Background()

	b.Add(ctx, tenantA, 10)
	if st, _ := b.Check(ctx, tenantB, 1); !st.Allowed {
		t.Error("tenant B must not see the spending of tenant A")
	}
}

// Many requests finishing at the same time must not lose any cost. Run with -race.
func TestBudgetAddIsAtomic(t *testing.T) {
	b, tenant := newTestBudget(t, false)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Add(ctx, tenant, 0.01); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	st, _ := b.Check(ctx, tenant, 100)
	if math.Abs(st.SpentUSD-1.00) > 1e-9 {
		t.Errorf("100 x 0.01 should be 1.00, got %v", st.SpentUSD)
	}
}

func TestBudgetAddIgnoresZeroAndNegative(t *testing.T) {
	b, tenant := newTestBudget(t, false)
	ctx := context.Background()
	b.Add(ctx, tenant, 0)
	b.Add(ctx, tenant, -5)
	if st, _ := b.Check(ctx, tenant, 1); st.SpentUSD != 0 {
		t.Errorf("nothing should be added: %+v", st)
	}
}

// ---- Redis down (no real Redis needed) ----

func deadRedis() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: 0, DialTimeout: 200 * time.Millisecond})
}

func TestBudgetRedisDownFailClosed(t *testing.T) {
	b := NewBudget(deadRedis(), false, 300*time.Millisecond)
	st, err := b.Check(context.Background(), "t1", 1)
	if !errors.Is(err, ErrUnavailable) || st.Allowed {
		t.Errorf("fail-closed must return ErrUnavailable: %+v, %v", st, err)
	}
}

func TestBudgetRedisDownFailOpen(t *testing.T) {
	b := NewBudget(deadRedis(), true, 300*time.Millisecond)
	st, err := b.Check(context.Background(), "t1", 1)
	if err != nil || !st.Allowed || !st.Degraded {
		t.Errorf("fail-open must allow and mark degraded: %+v, %v", st, err)
	}
}
