package cache

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"gatellm/internal/embed"
)

func TestVectorLiteral(t *testing.T) {
	if got := vectorLiteral([]float32{0.5, -1, 0.25}); got != "[0.5,-1,0.25]" {
		t.Errorf("got %s", got)
	}
	if got := vectorLiteral(nil); got != "[]" {
		t.Errorf("got %s", got)
	}
}

// ---- Postgres with pgvector (set TEST_DATABASE_URL to run) ----

func newTestPostgres(t *testing.T, threshold float64, ttl time.Duration) (*Postgres, string) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set, skipping the pgvector tests")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	ctx := context.Background()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("Postgres is not reachable: %v", err)
	}
	return NewPostgres(pool, threshold, ttl, 2*time.Second), newTestTenant(t, pool)
}

// newTestTenant adds a tenant row (semantic_cache has a foreign key to tenants).
// Deleting the tenant at the end also deletes its cache rows (ON DELETE CASCADE).
func newTestTenant(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var id string
	// gen_random_uuid() is built into Postgres 13 and newer.
	err := pool.QueryRow(context.Background(),
		`INSERT INTO tenants (id, name, rpm_limit, tpm_limit, monthly_budget_usd)
		 VALUES (gen_random_uuid(), 'semantic-test-' || gen_random_uuid()::text, 60, 10000, 5) RETURNING id::text`).Scan(&id)
	if err != nil {
		t.Fatalf("could not create a test tenant (is the database migrated?): %v", err)
	}
	t.Cleanup(func() { pool.Exec(context.Background(), `DELETE FROM tenants WHERE id = $1`, id) })
	return id
}

func vec(t *testing.T, text string) []float32 {
	t.Helper()
	v, err := embed.NewMock(768).Embed(context.Background(), text)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestSemanticFindsTheSameQuestion(t *testing.T) {
	c, tenant := newTestPostgres(t, 0.92, time.Hour)
	ctx := context.Background()
	q := "what is the capital of france"

	if _, _, hit, err := c.Find(ctx, tenant, "mock|0", vec(t, q)); hit || err != nil {
		t.Fatalf("empty table must be a clean miss: hit=%v err=%v", hit, err)
	}
	if err := c.Add(ctx, tenant, "mock|0", q, vec(t, q), testEntry("Paris")); err != nil {
		t.Fatal(err)
	}
	e, sim, hit, err := c.Find(ctx, tenant, "mock|0", vec(t, "What is the capital of France?"))
	if err != nil || !hit {
		t.Fatalf("expected a hit: hit=%v sim=%v err=%v", hit, sim, err)
	}
	if sim < 0.999 {
		t.Errorf("same words should be almost 1, got %v", sim)
	}
	if e.Response.Choices[0].Message.Content != "Paris" || e.Provider != "mock" {
		t.Errorf("wrong entry: %+v", e)
	}
}

func TestSemanticThresholdDecides(t *testing.T) {
	ctx := context.Background()
	stored := "what is the capital of france"
	asked := "tell me the capital of france please"

	c, tenant := newTestPostgres(t, 0.0, time.Hour)
	c.Add(ctx, tenant, "s", stored, vec(t, stored), testEntry("Paris"))
	_, sim, hit, err := c.Find(ctx, tenant, "s", vec(t, asked))
	if err != nil || !hit {
		t.Fatalf("with threshold 0 the closest row always counts: %v %v", hit, err)
	}
	if sim <= 0.3 || sim >= 0.999 {
		t.Fatalf("test needs a 'similar but not equal' pair, similarity is %v", sim)
	}

	c.threshold = sim + 0.01 // just above: must now be a miss
	if _, _, hit, _ := c.Find(ctx, tenant, "s", vec(t, asked)); hit {
		t.Error("a similarity below the threshold must be a miss")
	}
	c.threshold = sim - 0.01 // just below: hit
	if _, _, hit, _ := c.Find(ctx, tenant, "s", vec(t, asked)); !hit {
		t.Error("a similarity above the threshold must be a hit")
	}
}

func TestSemanticUnrelatedQuestionIsAMiss(t *testing.T) {
	c, tenant := newTestPostgres(t, 0.92, time.Hour)
	ctx := context.Background()
	c.Add(ctx, tenant, "s", "capital of france", vec(t, "capital of france"), testEntry("Paris"))
	if _, _, hit, _ := c.Find(ctx, tenant, "s", vec(t, "how to bake sourdough bread")); hit {
		t.Error("an unrelated question must be a miss")
	}
}

func TestSemanticPicksTheClosestOfSeveral(t *testing.T) {
	c, tenant := newTestPostgres(t, 0.5, time.Hour)
	ctx := context.Background()
	for _, q := range []string{"capital of germany", "capital of france", "capital of spain"} {
		c.Add(ctx, tenant, "s", q, vec(t, q), testEntry("answer for "+q))
	}
	e, _, hit, err := c.Find(ctx, tenant, "s", vec(t, "capital of france"))
	if err != nil || !hit || !strings.Contains(e.Response.Choices[0].Message.Content, "france") {
		t.Errorf("must return the closest row, got %+v hit=%v err=%v", e, hit, err)
	}
}

func TestSemanticTenantsAndScopesAreSeparate(t *testing.T) {
	c, tenantA := newTestPostgres(t, 0.92, time.Hour)
	tenantB := newTestTenant(t, c.pool)
	ctx := context.Background()
	q := "secret question of tenant a"
	c.Add(ctx, tenantA, "mock|0", q, vec(t, q), testEntry("secret answer"))

	if _, _, hit, _ := c.Find(ctx, tenantB, "mock|0", vec(t, q)); hit {
		t.Error("tenant B must never get tenant A's answer")
	}
	if _, _, hit, _ := c.Find(ctx, tenantA, "other-model|0", vec(t, q)); hit {
		t.Error("another model/scope must not share answers")
	}
	if _, _, hit, _ := c.Find(ctx, tenantA, "mock|0", vec(t, q)); !hit {
		t.Error("the owner must still get it")
	}
}

func TestSemanticOldRowsAreIgnoredAndCleaned(t *testing.T) {
	c, tenant := newTestPostgres(t, 0.92, 1500*time.Millisecond)
	ctx := context.Background()
	q := "short lived question"
	c.Add(ctx, tenant, "s", q, vec(t, q), testEntry("x"))
	if _, _, hit, _ := c.Find(ctx, tenant, "s", vec(t, q)); !hit {
		t.Fatal("fresh row must be found")
	}
	time.Sleep(1700 * time.Millisecond)
	if _, _, hit, _ := c.Find(ctx, tenant, "s", vec(t, q)); hit {
		t.Error("a row older than the TTL must be ignored")
	}
	if _, err := c.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	c.pool.QueryRow(ctx, `SELECT count(*) FROM semantic_cache WHERE tenant_id = $1`, tenant).Scan(&n)
	if n != 0 {
		t.Errorf("Cleanup must delete the old row, %d left", n)
	}
}

func TestSemanticWrongVectorSizeIsAnError(t *testing.T) {
	c, tenant := newTestPostgres(t, 0.92, time.Hour)
	if err := c.Add(context.Background(), tenant, "s", "q", []float32{1, 0, 0}, testEntry("x")); err == nil {
		t.Error("a vector of the wrong size must be refused by the database")
	}
}

func TestSemanticDamagedRowIsAnErrorNotAHit(t *testing.T) {
	c, tenant := newTestPostgres(t, 0.5, time.Hour)
	ctx := context.Background()
	q := "damaged row"
	_, err := c.pool.Exec(ctx,
		`INSERT INTO semantic_cache (tenant_id, model, prompt_text, embedding, response_json) VALUES ($1,'s',$2,$3::vector,'{"provider":"x"}')`,
		tenant, q, vectorLiteral(vec(t, q)))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, hit, err := c.Find(ctx, tenant, "s", vec(t, q)); hit || err == nil {
		t.Errorf("a row without a response must not be served: hit=%v err=%v", hit, err)
	}
}
