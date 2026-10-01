// Package config reads all settings from environment variables.
// No config files, no hardcoded secrets.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port        string // HTTP port, e.g. "8080"
	DatabaseURL string // Postgres connection string
	RedisURL    string // Redis connection string
	AdminToken  string // protects /admin/* endpoints (used from Phase 2)
	LogLevel    string // debug, info, warn, error

	MaxBodyBytes int64 // biggest request body we accept

	GroqAPIKey   string  // optional, groq is enabled only if set
	GeminiAPIKey string  // optional, gemini is enabled only if set
	MockLatency  int     // mock provider delay in milliseconds
	MockErrorPct float64 // mock provider failure rate: 0 (never) to 1 (always)

	RateLimitFailOpen  bool // what to do when Redis is down: true = let requests pass, false = reject them
	RateLimitTimeoutMS int  // max time for one rate limit check in Redis, in milliseconds

	// Reliability (Phase 5): timeouts, retries and circuit breaker, for every provider.
	ProviderTimeoutMS  int // max time for ONE attempt (for a stream: until it starts)
	GroqTimeoutMS      int // optional, overrides ProviderTimeoutMS for groq
	GeminiTimeoutMS    int // optional, overrides ProviderTimeoutMS for gemini
	RetryMaxAttempts   int // total tries, including the first one
	RetryBaseMS        int // wait before the first retry
	RetryMaxMS         int // the wait never grows above this
	BreakerFailures    int // failures in a row that open the circuit breaker
	BreakerCooldownSec int // how long the breaker stays open

	// Routing (Phase 5b): the model "auto".
	RouteStrategy      string  // priority, weighted or latency
	RouteTargets       string  // e.g. "groq/llama-3.1-8b-instant,gemini/gemini-2.5-flash" (empty = all real providers)
	HealthCheckSeconds int     // how often to ping the providers, 0 = never
	MockBLatency       int     // the second mock ("mock-b"), used to test fallback
	MockBErrorPct      float64 // failure rate of mock-b

	// Cache (Phase 6): exact-match answers in Redis.
	CacheEnabled   bool // switch the cache on or off
	CacheTTLSec    int  // how long an answer is kept
	CacheAllowTemp bool // also cache requests with temperature above 0 (or none)
	CacheTimeoutMS int  // max time for one cache call in Redis

	// Semantic cache (Phase 6b): "almost the same question" found with pgvector.
	SemanticEnabled   bool
	SemanticThreshold float64 // minimum cosine similarity for a hit (0 to 1)
	SemanticTimeoutMS int     // max time for one pgvector query
	EmbeddingProvider string  // "mock" (no key needed) or "gemini"
	EmbeddingModel    string  // for gemini, e.g. gemini-embedding-001
	EmbeddingTimeout  int     // max time for one embedding call, in milliseconds
}

// Load reads env variables and checks that the required ones exist.
func Load() (*Config, error) {
	cfg := &Config{
		Port:         getEnv("PORT", "8080"),
		DatabaseURL:  getEnv("DATABASE_URL", ""),
		RedisURL:     getEnv("REDIS_URL", ""),
		AdminToken:   getEnv("ADMIN_TOKEN", ""),
		LogLevel:     getEnv("LOG_LEVEL", "info"),
		GroqAPIKey:   getEnv("GROQ_API_KEY", ""),
		GeminiAPIKey: getEnv("GEMINI_API_KEY", ""),
	}

	// Collect every missing variable so the error message is helpful.
	var missing []string
	if cfg.DatabaseURL == "" {
		missing = append(missing, "DATABASE_URL")
	}
	if cfg.RedisURL == "" {
		missing = append(missing, "REDIS_URL")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required env variables: %s", strings.Join(missing, ", "))
	}

	// Numbers need to be converted from text.
	var err error
	if cfg.MaxBodyBytes, err = getInt64("MAX_BODY_BYTES", 1<<20); err != nil { // default 1 MB
		return nil, err
	}
	latency, err := getInt64("MOCK_LATENCY_MS", 200)
	if err != nil {
		return nil, err
	}
	cfg.MockLatency = int(latency)
	if cfg.MockErrorPct, err = getFloat("MOCK_ERROR_RATE", 0); err != nil {
		return nil, err
	}
	if cfg.MockErrorPct < 0 || cfg.MockErrorPct > 1 {
		return nil, fmt.Errorf("MOCK_ERROR_RATE must be between 0 and 1")
	}

	// What happens to requests when the rate limiter cannot reach Redis.
	switch mode := strings.ToLower(getEnv("RATE_LIMIT_ON_REDIS_DOWN", "closed")); mode {
	case "closed":
		cfg.RateLimitFailOpen = false
	case "open":
		cfg.RateLimitFailOpen = true
	default:
		return nil, fmt.Errorf("RATE_LIMIT_ON_REDIS_DOWN must be \"open\" or \"closed\", got %q", mode)
	}
	timeout, err := getInt64("RATE_LIMIT_TIMEOUT_MS", 500)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("RATE_LIMIT_TIMEOUT_MS must be bigger than 0")
	}
	cfg.RateLimitTimeoutMS = int(timeout)

	// Reliability settings. Each one must be a positive number (the provider timeouts may be 0 = use the default).
	for _, s := range []struct {
		name     string
		fallback int64
		dest     *int
		allow0   bool
	}{
		{"PROVIDER_TIMEOUT_MS", 30000, &cfg.ProviderTimeoutMS, false},
		{"GROQ_TIMEOUT_MS", 0, &cfg.GroqTimeoutMS, true},
		{"GEMINI_TIMEOUT_MS", 0, &cfg.GeminiTimeoutMS, true},
		{"RETRY_MAX_ATTEMPTS", 3, &cfg.RetryMaxAttempts, false},
		{"RETRY_BASE_MS", 200, &cfg.RetryBaseMS, false},
		{"RETRY_MAX_MS", 2000, &cfg.RetryMaxMS, false},
		{"BREAKER_FAILURES", 5, &cfg.BreakerFailures, false},
		{"BREAKER_COOLDOWN_SECONDS", 30, &cfg.BreakerCooldownSec, false},
	} {
		n, err := getInt64(s.name, s.fallback)
		if err != nil {
			return nil, err
		}
		if n < 0 || (n == 0 && !s.allow0) {
			return nil, fmt.Errorf("%s must be bigger than 0", s.name)
		}
		*s.dest = int(n)
	}

	cfg.RouteStrategy = strings.ToLower(getEnv("ROUTE_STRATEGY", "priority"))
	cfg.RouteTargets = getEnv("ROUTE_TARGETS", "")

	health, err := getInt64("HEALTH_CHECK_INTERVAL_SECONDS", 30)
	if err != nil {
		return nil, err
	}
	if health < 0 {
		return nil, fmt.Errorf("HEALTH_CHECK_INTERVAL_SECONDS must be 0 (off) or more")
	}
	cfg.HealthCheckSeconds = int(health)

	mockBLatency, err := getInt64("MOCK_B_LATENCY_MS", 200)
	if err != nil {
		return nil, err
	}
	cfg.MockBLatency = int(mockBLatency)
	if cfg.MockBErrorPct, err = getFloat("MOCK_B_ERROR_RATE", 0); err != nil {
		return nil, err
	}
	if cfg.MockBErrorPct < 0 || cfg.MockBErrorPct > 1 {
		return nil, fmt.Errorf("MOCK_B_ERROR_RATE must be between 0 and 1")
	}

	if cfg.CacheEnabled, err = getBool("CACHE_ENABLED", true); err != nil {
		return nil, err
	}
	if cfg.CacheAllowTemp, err = getBool("CACHE_ALLOW_TEMPERATURE", false); err != nil {
		return nil, err
	}
	ttl, err := getInt64("CACHE_TTL_SECONDS", 3600)
	if err != nil {
		return nil, err
	}
	cacheTimeout, err := getInt64("CACHE_TIMEOUT_MS", 200)
	if err != nil {
		return nil, err
	}
	if ttl <= 0 || cacheTimeout <= 0 {
		return nil, fmt.Errorf("CACHE_TTL_SECONDS and CACHE_TIMEOUT_MS must be bigger than 0")
	}
	cfg.CacheTTLSec, cfg.CacheTimeoutMS = int(ttl), int(cacheTimeout)

	// Semantic cache. It is off by default: it needs an embedding provider.
	if cfg.SemanticEnabled, err = getBool("SEMANTIC_CACHE_ENABLED", false); err != nil {
		return nil, err
	}
	if cfg.SemanticThreshold, err = getFloat("SEMANTIC_THRESHOLD", 0.92); err != nil {
		return nil, err
	}
	if cfg.SemanticThreshold <= 0 || cfg.SemanticThreshold > 1 {
		return nil, fmt.Errorf("SEMANTIC_THRESHOLD must be bigger than 0 and at most 1")
	}
	semTimeout, err := getInt64("SEMANTIC_TIMEOUT_MS", 1000)
	if err != nil {
		return nil, err
	}
	embTimeout, err := getInt64("EMBEDDING_TIMEOUT_MS", 3000)
	if err != nil {
		return nil, err
	}
	if semTimeout <= 0 || embTimeout <= 0 {
		return nil, fmt.Errorf("SEMANTIC_TIMEOUT_MS and EMBEDDING_TIMEOUT_MS must be bigger than 0")
	}
	cfg.SemanticTimeoutMS, cfg.EmbeddingTimeout = int(semTimeout), int(embTimeout)
	cfg.EmbeddingProvider = strings.ToLower(getEnv("EMBEDDING_PROVIDER", "mock"))
	cfg.EmbeddingModel = getEnv("EMBEDDING_MODEL", "gemini-embedding-001")
	if cfg.SemanticEnabled {
		if !cfg.CacheEnabled {
			return nil, fmt.Errorf("SEMANTIC_CACHE_ENABLED=true needs CACHE_ENABLED=true")
		}
		switch cfg.EmbeddingProvider {
		case "mock":
		case "gemini":
			if cfg.GeminiAPIKey == "" {
				return nil, fmt.Errorf("EMBEDDING_PROVIDER=gemini needs GEMINI_API_KEY")
			}
		default:
			return nil, fmt.Errorf("EMBEDDING_PROVIDER must be \"mock\" or \"gemini\", got %q", cfg.EmbeddingProvider)
		}
	}
	return cfg, nil
}

// getEnv returns the trimmed env value or a default when it is empty.
// TrimSpace removes hidden spaces and line breaks from copy-paste.
func getEnv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func getInt64(key string, fallback int64) (int64, error) {
	v := getEnv(key, "")
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a whole number, got %q", key, v)
	}
	return n, nil
}

func getFloat(key string, fallback float64) (float64, error) {
	v := getEnv(key, "")
	if v == "" {
		return fallback, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number, got %q", key, v)
	}
	return f, nil
}

func getBool(key string, fallback bool) (bool, error) {
	v := getEnv(key, "")
	if v == "" {
		return fallback, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false, got %q", key, v)
	}
	return b, nil
}
