-- 001_init.sql : all core tables for GateLLM

CREATE EXTENSION IF NOT EXISTS vector;

-- One row per customer (tenant) of the gateway.
CREATE TABLE IF NOT EXISTS tenants (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name               TEXT NOT NULL,
    rpm_limit          INT NOT NULL DEFAULT 60,        -- requests per minute
    tpm_limit          INT NOT NULL DEFAULT 20000,     -- tokens per minute
    monthly_budget_usd NUMERIC(10,4) NOT NULL DEFAULT 5,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- We store ONLY the SHA-256 hash of the API key, never the raw key.
CREATE TABLE IF NOT EXISTS api_keys (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id  UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    key_hash   TEXT NOT NULL UNIQUE,
    active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per request. request_id UNIQUE makes writes idempotent (Phase 7).
CREATE TABLE IF NOT EXISTS usage_events (
    request_id        TEXT PRIMARY KEY,
    tenant_id         UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    provider          TEXT NOT NULL,
    model             TEXT NOT NULL,
    prompt_tokens     INT NOT NULL DEFAULT 0,
    completion_tokens INT NOT NULL DEFAULT 0,
    cost_usd          NUMERIC(12,6) NOT NULL DEFAULT 0,
    latency_ms        INT NOT NULL DEFAULT 0,
    cache_status      TEXT NOT NULL DEFAULT 'MISS',    -- HIT-EXACT / HIT-SEMANTIC / MISS
    status            TEXT NOT NULL DEFAULT 'ok',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_usage_tenant_time ON usage_events (tenant_id, created_at);

-- Semantic cache. vector(768) is a placeholder size, we confirm the real
-- embedding dimension in Phase 6 when we pick the embedding API.
CREATE TABLE IF NOT EXISTS semantic_cache (
    id            BIGSERIAL PRIMARY KEY,
    tenant_id     UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    model         TEXT NOT NULL,
    prompt_text   TEXT NOT NULL,
    embedding     vector(768) NOT NULL,
    response_json JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_semantic_embedding
    ON semantic_cache USING hnsw (embedding vector_cosine_ops);
