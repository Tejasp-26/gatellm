package usage

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newTestDB connects to a migrated Postgres. Set TEST_DATABASE_URL to run these tests.
func newTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set, skipping the Postgres tests")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		t.Skipf("Postgres is not reachable: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// newTenant adds a tenant (usage_events has a foreign key to it). Deleting it removes its events too.
func newTenant(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var id string
	err := pool.QueryRow(context.Background(),
		`INSERT INTO tenants (id, name, rpm_limit, tpm_limit, monthly_budget_usd)
		 VALUES (gen_random_uuid(), 'usage-test-' || gen_random_uuid()::text, 60, 10000, 5) RETURNING id::text`).Scan(&id)
	if err != nil {
		t.Fatalf("could not create a tenant (is the database migrated?): %v", err)
	}
	t.Cleanup(func() { pool.Exec(context.Background(), `DELETE FROM tenants WHERE id = $1`, id) })
	return id
}

func countRows(t *testing.T, pool *pgxpool.Pool, tenant string) int {
	t.Helper()
	var n int
	pool.QueryRow(context.Background(), `SELECT count(*) FROM usage_events WHERE tenant_id = $1`, tenant).Scan(&n)
	return n
}

func TestPGWriterStoresEveryField(t *testing.T) {
	pool := newTestDB(t)
	tenant := newTenant(t, pool)
	e := sampleEvent("pg-fields-" + tenant)
	e.TenantID = tenant

	if err := NewPGWriter(pool).Write(context.Background(), []Event{e}); err != nil {
		t.Fatal(err)
	}
	var got Event
	var cost float64
	err := pool.QueryRow(context.Background(),
		`SELECT request_id, tenant_id::text, provider, model, prompt_tokens, completion_tokens, cost_usd::float8, latency_ms, cache_status, status, created_at
		 FROM usage_events WHERE request_id = $1`, e.RequestID).
		Scan(&got.RequestID, &got.TenantID, &got.Provider, &got.Model, &got.PromptTokens, &got.CompletionTokens, &cost, &got.LatencyMS, &got.CacheStatus, &got.Status, &got.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	got.CostUSD = cost
	if math.Abs(got.CostUSD-e.CostUSD) > 1e-9 {
		t.Errorf("cost %v, want %v", got.CostUSD, e.CostUSD)
	}
	got.CostUSD, e.CostUSD = 0, 0
	if !got.CreatedAt.Equal(e.CreatedAt) {
		t.Errorf("created_at %v, want %v", got.CreatedAt, e.CreatedAt)
	}
	got.CreatedAt, e.CreatedAt = time.Time{}, time.Time{}
	if got != e {
		t.Errorf("stored differently:\n got  %+v\n want %+v", got, e)
	}
}

// The same request_id twice (a message delivered again after a crash) must give ONE row.
func TestPGWriterIgnoresDuplicates(t *testing.T) {
	pool := newTestDB(t)
	tenant := newTenant(t, pool)
	w := NewPGWriter(pool)
	e := sampleEvent("dup-" + tenant)
	e.TenantID = tenant

	if err := w.Write(context.Background(), []Event{e}); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(context.Background(), []Event{e}); err != nil {
		t.Fatalf("a duplicate is not an error: %v", err)
	}
	other := sampleEvent("dup2-" + tenant)
	other.TenantID = tenant
	if err := w.Write(context.Background(), []Event{other, other, e}); err != nil { // duplicates inside one batch too
		t.Fatal(err)
	}
	if n := countRows(t, pool, tenant); n != 2 {
		t.Errorf("expected 2 rows, found %d", n)
	}
}

func TestPGWriterBatchIsAllOrNothing(t *testing.T) {
	pool := newTestDB(t)
	tenant := newTenant(t, pool)
	good := sampleEvent("aon-good-" + tenant)
	good.TenantID = tenant
	bad := sampleEvent("aon-bad-" + tenant)
	bad.TenantID = "22222222-2222-2222-2222-222222222222" // no such tenant

	err := NewPGWriter(pool).Write(context.Background(), []Event{good, bad})
	if err == nil || !IsPermanent(err) {
		t.Fatalf("a missing tenant is a permanent error, got %v", err)
	}
	if n := countRows(t, pool, tenant); n != 0 {
		t.Errorf("the good event must be rolled back with the bad one, found %d rows", n)
	}
}

func TestPGWriterConnectionProblemIsNotPermanent(t *testing.T) {
	pool := newTestDB(t)
	pool.Close() // like a lost database connection
	err := NewPGWriter(pool).Write(context.Background(), []Event{sampleEvent("closed")})
	if err == nil {
		t.Fatal("expected an error")
	}
	if IsPermanent(err) {
		t.Error("a connection problem must be retried, not dead-lettered")
	}
}

// The whole path: Recorder -> Redis Stream -> Consumer -> Postgres.
func TestEndToEndStreamToDatabase(t *testing.T) {
	rdb := newRedis(t)
	pool := newTestDB(t)
	tenant := newTenant(t, pool)
	w := NewPGWriter(pool)

	rec := NewRecorder(NewStream(rdb, 10000), w, time.Second)
	const n = 100
	for i := 0; i < n; i++ {
		e := sampleEvent(fmt.Sprintf("e2e-%s-%d", tenant, i))
		e.TenantID = tenant
		rec.Record(context.Background(), e)
		if i%10 == 0 { // some events arrive twice, like after a crash
			rec.Record(context.Background(), e)
		}
	}

	stop := startConsumer(t, NewConsumer(rdb, w, fastConfig("e2e")), 5*time.Second)
	waitFor(t, "all events in Postgres", func() bool { return countRows(t, pool, tenant) == n })
	stop()

	if got := countRows(t, pool, tenant); got != n {
		t.Errorf("%d rows, want exactly %d (duplicates must be ignored)", got, n)
	}
	if p := pending(t, rdb); p != 0 {
		t.Errorf("%d pending", p)
	}
}
