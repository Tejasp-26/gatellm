package config

import "testing"

// baseEnv sets the settings that are always required.
func baseEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("REDIS_URL", "redis://x")
}

func TestSemanticCacheIsOffByDefault(t *testing.T) {
	baseEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SemanticEnabled || cfg.SemanticThreshold != 0.92 || cfg.EmbeddingProvider != "mock" {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
	if !cfg.CacheEnabled || cfg.CacheTTLSec != 3600 || cfg.CacheAllowTemp {
		t.Errorf("unexpected cache defaults: %+v", cfg)
	}
}

func TestSemanticCacheSettingsAreChecked(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		ok   bool
	}{
		{"mock embeddings", map[string]string{"SEMANTIC_CACHE_ENABLED": "true"}, true},
		{"gemini without key", map[string]string{"SEMANTIC_CACHE_ENABLED": "true", "EMBEDDING_PROVIDER": "gemini"}, false},
		{"gemini with key", map[string]string{"SEMANTIC_CACHE_ENABLED": "true", "EMBEDDING_PROVIDER": "gemini", "GEMINI_API_KEY": "k"}, true},
		{"unknown provider", map[string]string{"SEMANTIC_CACHE_ENABLED": "true", "EMBEDDING_PROVIDER": "openai"}, false},
		{"needs the exact cache", map[string]string{"SEMANTIC_CACHE_ENABLED": "true", "CACHE_ENABLED": "false"}, false},
		{"threshold too high", map[string]string{"SEMANTIC_THRESHOLD": "1.5"}, false},
		{"threshold zero", map[string]string{"SEMANTIC_THRESHOLD": "0"}, false},
		{"threshold not a number", map[string]string{"SEMANTIC_THRESHOLD": "high"}, false},
		{"bad bool", map[string]string{"CACHE_ENABLED": "maybe"}, false},
		{"ttl zero", map[string]string{"CACHE_TTL_SECONDS": "0"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			baseEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			if (err == nil) != tc.ok {
				t.Errorf("error = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestUsagePipelineDefaultsAndChecks(t *testing.T) {
	baseEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.UsageEnabled || !cfg.UsageBlocking || cfg.UsageBatch != 50 || cfg.UsagePollMS != 5000 ||
		cfg.UsageDrainSec != 10 || cfg.UsageStreamMaxLen != 100000 || cfg.UsageClaimIdleSec != 30 {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
	for _, bad := range []map[string]string{
		{"USAGE_BATCH_SIZE": "0"}, {"USAGE_DRAIN_SECONDS": "-1"}, {"USAGE_POLL_MS": "soon"}, {"USAGE_BLOCKING_READ": "maybe"},
	} {
		baseEnv(t)
		for k, v := range bad {
			t.Setenv(k, v)
		}
		if _, err := Load(); err == nil {
			t.Errorf("%v should be refused", bad)
		}
	}
}
