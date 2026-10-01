package usage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrUnavailable means Redis could not be used and we are in fail-closed mode.
var ErrUnavailable = errors.New("budget tracker unavailable")

// BudgetStatus is the answer of Check.
type BudgetStatus struct {
	Allowed      bool
	SpentUSD     float64
	RemainingUSD float64
	Degraded     bool // true: Redis was down and we let the request pass (fail-open)
}

// Budget keeps the money each tenant spent in the current month, in Redis.
// One key per tenant and month, like "budget:{tenant-id}:2026-10".
// A new month means a new key, so the budget resets by itself.
type Budget struct {
	rdb      *redis.Client
	failOpen bool
	timeout  time.Duration
	now      func() time.Time
}

// NewBudget creates the tracker. failOpen works like in the rate limiter.
func NewBudget(rdb *redis.Client, failOpen bool, timeout time.Duration) *Budget {
	return &Budget{rdb: rdb, failOpen: failOpen, timeout: timeout, now: time.Now}
}

// budgetKey uses the UTC month, so every server agrees when the month changes.
func budgetKey(tenantID string, now time.Time) string {
	return "budget:{" + tenantID + "}:" + now.UTC().Format("2006-01")
}

// Check says if the tenant may still spend money this month.
// It is a check on what was spent SO FAR (see the trade-off in the explanation).
// A budget of 0 means nothing may be spent.
func (b *Budget) Check(ctx context.Context, tenantID string, budgetUSD float64) (BudgetStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	spent, err := b.rdb.Get(ctx, budgetKey(tenantID, b.now())).Float64()
	if errors.Is(err, redis.Nil) {
		spent, err = 0, nil // nothing spent yet this month
	}
	if err != nil {
		if b.failOpen {
			return BudgetStatus{Allowed: true, Degraded: true}, nil
		}
		return BudgetStatus{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}

	remaining := budgetUSD - spent
	return BudgetStatus{
		Allowed:      spent < budgetUSD,
		SpentUSD:     spent,
		RemainingUSD: max(remaining, 0),
	}, nil
}

// Add puts the cost of a finished request on the tenant's counter.
// INCRBYFLOAT is atomic in Redis, so parallel requests cannot lose each other's cost.
func (b *Budget) Add(ctx context.Context, tenantID string, usd float64) error {
	if usd <= 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	now := b.now()
	key := budgetKey(tenantID, now)
	if err := b.rdb.IncrByFloat(ctx, key, usd).Err(); err != nil {
		return err
	}
	// Delete the key a week after the month ends. It is not needed any more.
	nextMonth := time.Date(now.UTC().Year(), now.UTC().Month()+1, 1, 0, 0, 0, 0, time.UTC)
	return b.rdb.ExpireAt(ctx, key, nextMonth.Add(7*24*time.Hour)).Err()
}

// FormatUSD is a small helper for headers and messages.
func FormatUSD(v float64) string {
	return strconv.FormatFloat(v, 'f', 6, 64)
}
