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
