package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound means the tenant or API key does not exist (or the key is switched off).
var ErrNotFound = errors.New("not found")

// Tenant is one customer of the gateway.
type Tenant struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	RPMLimit         int       `json:"rpm_limit"`          // requests per minute
	TPMLimit         int       `json:"tpm_limit"`          // tokens per minute
	MonthlyBudgetUSD float64   `json:"monthly_budget_usd"` // monthly spending limit
	CreatedAt        time.Time `json:"created_at"`
}

// Tenants is the database code for tenants and their API keys.
type Tenants struct {
	pool *pgxpool.Pool
}

func NewTenants(pool *pgxpool.Pool) *Tenants {
	return &Tenants{pool: pool}
}

// CreateTenant saves a new tenant and its first API key hash.
// Both inserts run in one transaction: either both are saved or neither.
func (t *Tenants) CreateTenant(ctx context.Context, name string, rpm, tpm int, budget float64, keyHash string) (*Tenant, error) {
	tx, err := t.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // does nothing after a successful Commit

	// The ::float8 and ::text casts keep the Go types simple (float64 and string).
	tenant := &Tenant{Name: name, RPMLimit: rpm, TPMLimit: tpm, MonthlyBudgetUSD: budget}
	err = tx.QueryRow(ctx,
		`INSERT INTO tenants (name, rpm_limit, tpm_limit, monthly_budget_usd)
		 VALUES ($1, $2, $3, $4::float8)
		 RETURNING id::text, created_at`,
		name, rpm, tpm, budget,
	).Scan(&tenant.ID, &tenant.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert tenant: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO api_keys (tenant_id, key_hash) VALUES ($1::text::uuid, $2)`,
		tenant.ID, keyHash)
	if err != nil {
		return nil, fmt.Errorf("insert api key: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return tenant, nil
}

// AddKey adds one more API key to an existing tenant.
// It returns ErrNotFound if the tenant does not exist.
func (t *Tenants) AddKey(ctx context.Context, tenantID, keyHash string) error {
	// INSERT ... SELECT inserts nothing when the tenant id does not exist.
	tag, err := t.pool.Exec(ctx,
		`INSERT INTO api_keys (tenant_id, key_hash)
		 SELECT id, $2 FROM tenants WHERE id = $1::text::uuid`,
		tenantID, keyHash)
	if err != nil {
		return fmt.Errorf("insert api key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// FindByKeyHash finds the tenant that owns an active API key.
// It returns ErrNotFound if the key is unknown or switched off.
func (t *Tenants) FindByKeyHash(ctx context.Context, keyHash string) (*Tenant, error) {
	var tenant Tenant
	err := t.pool.QueryRow(ctx,
		`SELECT t.id::text, t.name, t.rpm_limit, t.tpm_limit, t.monthly_budget_usd::float8, t.created_at
		 FROM api_keys k
		 JOIN tenants t ON t.id = k.tenant_id
		 WHERE k.key_hash = $1 AND k.active = TRUE`,
		keyHash,
	).Scan(&tenant.ID, &tenant.Name, &tenant.RPMLimit, &tenant.TPMLimit, &tenant.MonthlyBudgetUSD, &tenant.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("find tenant by key: %w", err)
	}
	return &tenant, nil
}
