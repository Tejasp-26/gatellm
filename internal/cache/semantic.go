package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres is the Semantic cache. It uses the table semantic_cache (see migrations).
//
// "<=>" is the pgvector operator for cosine DISTANCE (0 = same direction, 2 = opposite).
// Similarity = 1 - distance. The HNSW index on the embedding column makes the search fast.
type Postgres struct {
	pool      *pgxpool.Pool
	threshold float64       // minimum similarity for a hit, for example 0.92
	ttl       time.Duration // rows older than this are ignored (and deleted by Cleanup)
	timeout   time.Duration // max time for one query: the cache must never slow requests down
}

func NewPostgres(pool *pgxpool.Pool, threshold float64, ttl, timeout time.Duration) *Postgres {
	return &Postgres{pool: pool, threshold: threshold, ttl: ttl, timeout: timeout}
}

// vectorLiteral writes a vector the way pgvector reads it: "[0.1,0.2,0.3]".
// We send it as text and cast it with ::vector, so we need no extra library.
func vectorLiteral(v []float32) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(x), 'g', 8, 32))
	}
	b.WriteByte(']')
	return b.String()
}

const findSQL = `
SELECT response_json, 1 - (embedding <=> $1::vector) AS similarity
FROM semantic_cache
WHERE tenant_id = $2
  AND model = $3
  AND created_at > now() - make_interval(secs => $4)
ORDER BY embedding <=> $1::vector
LIMIT 1`

func (c *Postgres) Find(ctx context.Context, tenantID, scope string, vec []float32) (*Entry, float64, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var raw []byte
	var similarity float64
	err := c.pool.QueryRow(ctx, findSQL, vectorLiteral(vec), tenantID, scope, c.ttl.Seconds()).Scan(&raw, &similarity)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, false, nil // nothing stored yet
	}
	if err != nil {
		return nil, 0, false, err
	}
	if similarity < c.threshold {
		return nil, similarity, false, nil // the closest one is not close enough
	}

	var entry Entry
	if err := json.Unmarshal(raw, &entry); err != nil || entry.Response == nil {
		return nil, similarity, false, fmt.Errorf("stored semantic entry is damaged: %v", err)
	}
	return &entry, similarity, true, nil
}

func (c *Postgres) Add(ctx context.Context, tenantID, scope, promptText string, vec []float32, entry *Entry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if len(data) > maxEntryBytes {
		return nil // too big, we just do not cache it
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	_, err = c.pool.Exec(ctx,
		`INSERT INTO semantic_cache (tenant_id, model, prompt_text, embedding, response_json)
		 VALUES ($1, $2, $3, $4::vector, $5)`,
		tenantID, scope, promptText, vectorLiteral(vec), data)
	return err
}

// Cleanup deletes rows older than the TTL and returns how many it deleted.
// Find already ignores them, this only keeps the table (and the free Neon storage) small.
func (c *Postgres) Cleanup(ctx context.Context) (int64, error) {
	tag, err := c.pool.Exec(ctx,
		`DELETE FROM semantic_cache WHERE created_at < now() - make_interval(secs => $1)`, c.ttl.Seconds())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
