// Package config reads all settings from environment variables.
// No config files, no hardcoded secrets.
package config

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	Port        string // HTTP port, e.g. "8080"
	DatabaseURL string // Postgres connection string
	RedisURL    string // Redis connection string
	AdminToken  string // protects /admin/* endpoints (used from Phase 2)
	LogLevel    string // debug, info, warn, error
}

// Load reads env variables and checks that the required ones exist.
func Load() (*Config, error) {
	cfg := &Config{
		Port:        getEnv("PORT", "8080"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		RedisURL:    os.Getenv("REDIS_URL"),
		AdminToken:  os.Getenv("ADMIN_TOKEN"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
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
	return cfg, nil
}

// getEnv returns the env value or a default when it is empty.
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
