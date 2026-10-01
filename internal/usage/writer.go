package usage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EventWriter saves events permanently. The real one is PGWriter. Tests use a fake.
type EventWriter interface {
	Write(ctx context.Context, events []Event) error
}

// permanentError marks an error that will happen again if we retry
// (for example: the tenant does not exist any more). Such an event goes to the dead-letter stream.
type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// IsPermanent says if retrying is pointless.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// PGWriter writes events to the table usage_events.
type PGWriter struct{ pool *pgxpool.Pool }

func NewPGWriter(pool *pgxpool.Pool) *PGWriter { return &PGWriter{pool: pool} }

// ON CONFLICT DO NOTHING is the idempotency: if the same request_id arrives twice
// (for example because a message was delivered again after a crash), the second one is ignored.
const insertSQL = `
INSERT INTO usage_events
  (request_id, tenant_id, provider, model, prompt_tokens, completion_tokens, cost_usd, latency_ms, cache_status, status, created_at)
VALUES ($1, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (request_id) DO NOTHING`

// Write saves all events in ONE transaction: either all are saved or none.
func (w *PGWriter) Write(ctx context.Context, events []Event) error {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // does nothing after a successful Commit

	batch := &pgx.Batch{} // all inserts travel to the database in one round trip
	for _, e := range events {
		batch.Queue(insertSQL, e.RequestID, e.TenantID, e.Provider, e.Model,
			e.PromptTokens, e.CompletionTokens, e.CostUSD, e.LatencyMS, e.CacheStatus, e.Status, e.CreatedAt)
	}
	results := tx.SendBatch(ctx, batch)
	for range events {
		if _, err := results.Exec(); err != nil {
			results.Close()
			return classify(err)
		}
	}
	if err := results.Close(); err != nil {
		return classify(err)
	}
	return classify(tx.Commit(ctx))
}

// classify decides if a database error is permanent.
// Postgres error classes: 22 = bad data, 23 = a constraint was violated (for example a missing tenant).
// Everything else (connection lost, timeout, ...) may work later, so it is NOT permanent.
func classify(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) >= 2 && (pgErr.Code[:2] == "22" || pgErr.Code[:2] == "23") {
		return &permanentError{err: err}
	}
	return err
}
