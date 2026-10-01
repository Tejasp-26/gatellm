package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gatellm/internal/api"
	"gatellm/internal/breaker"
	"gatellm/internal/cache"
	"gatellm/internal/config"
	"gatellm/internal/embed"
	"gatellm/internal/provider"
	"gatellm/internal/ratelimit"
	"gatellm/internal/router"
	"gatellm/internal/store"
	"gatellm/internal/usage"
	"gatellm/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("gateway stopped with error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	// 1. Load config from env variables.
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// 2. Set up structured JSON logging.
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	// ctx is cancelled when we receive Ctrl+C or SIGTERM (Docker/Render stop signal).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 3. Connect to Postgres and Redis.
	db, err := store.NewPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	rdb, err := store.NewRedis(ctx, cfg.RedisURL)
	if err != nil {
		return err
	}
	defer rdb.Close()

	// 4. Run migrations at startup.
	if err := store.RunMigrations(ctx, db, migrations.FS); err != nil {
		return err
	}

	// 5. Build the providers. Mock is always on. Groq and Gemini need an API key.
	// Every provider is wrapped with timeout + retry + circuit breaker.
	// Each provider gets its OWN breaker, so a broken Groq does not block Gemini.
	protect := func(p provider.Provider, timeoutMS int) provider.Provider {
		if timeoutMS == 0 {
			timeoutMS = cfg.ProviderTimeoutMS // no special timeout for this provider
		}
		return provider.NewResilient(p, provider.ResilientConfig{
			Timeout: time.Duration(timeoutMS) * time.Millisecond,
			Retry: provider.RetryConfig{
				MaxAttempts: cfg.RetryMaxAttempts,
				BaseDelay:   time.Duration(cfg.RetryBaseMS) * time.Millisecond,
				MaxDelay:    time.Duration(cfg.RetryMaxMS) * time.Millisecond,
			},
			Breaker: breaker.Config{
				FailureThreshold: cfg.BreakerFailures,
				Cooldown:         time.Duration(cfg.BreakerCooldownSec) * time.Second,
			},
		})
	}
	providers := provider.Registry{
		"mock": protect(provider.NewMock(time.Duration(cfg.MockLatency)*time.Millisecond, cfg.MockErrorPct), 0),
		// A second mock. With two mocks we can test fallback: make one fail, the other answers.
		"mock-b": protect(provider.NewMockNamed("mock-b", time.Duration(cfg.MockBLatency)*time.Millisecond, cfg.MockBErrorPct), 0),
	}
	if cfg.GroqAPIKey != "" {
		providers["groq"] = protect(provider.NewGroq(cfg.GroqAPIKey), cfg.GroqTimeoutMS)
	}
	if cfg.GeminiAPIKey != "" {
		providers["gemini"] = protect(provider.NewGemini(cfg.GeminiAPIKey), cfg.GeminiTimeoutMS)
	}
	slog.Info("providers enabled", "names", providers.Names())

	// 5b. The router handles the model "auto": it picks a provider and falls back to the next one.
	strategy, err := router.NewStrategy(cfg.RouteStrategy)
	if err != nil {
		return err
	}
	spec := cfg.RouteTargets
	if spec == "" {
		spec = router.DefaultTargetSpec(providers)
	}
	targets, err := router.ParseTargets(spec, providers)
	if err != nil {
		return err
	}
	autoRouter := router.New(targets, strategy)
	slog.Info("router ready", "strategy", cfg.RouteStrategy, "targets", spec)

	// Ping the providers in the background (this costs no tokens). It stops when we shut down.
	if cfg.HealthCheckSeconds > 0 {
		go autoRouter.RunHealthChecks(ctx, time.Duration(cfg.HealthCheckSeconds)*time.Second)
	}

	// 6. Start the HTTP server.
	if cfg.AdminToken == "" {
		slog.Warn("ADMIN_TOKEN is not set, the /admin endpoints are disabled")
	}
	handler := &api.Handler{
		DB:         db,
		Redis:      rdb,
		Providers:  providers,
		Tenants:    store.NewTenants(db),
		AdminToken: cfg.AdminToken,
		Router:     autoRouter,
		Limiter: ratelimit.New(rdb, cfg.RateLimitFailOpen,
			time.Duration(cfg.RateLimitTimeoutMS)*time.Millisecond),
		Budget: usage.NewBudget(rdb, cfg.RateLimitFailOpen,
			time.Duration(cfg.RateLimitTimeoutMS)*time.Millisecond),
	}
	if cfg.CacheEnabled {
		handler.Cache = cache.NewRedis(rdb,
			time.Duration(cfg.CacheTTLSec)*time.Second,
			time.Duration(cfg.CacheTimeoutMS)*time.Millisecond)
		handler.CacheAllowTemperature = cfg.CacheAllowTemp
	}
	if cfg.SemanticEnabled {
		var embedder embed.Embedder
		if cfg.EmbeddingProvider == "gemini" {
			embedder = embed.NewOpenAICompatible(embed.GeminiBaseURL, cfg.GeminiAPIKey, cfg.EmbeddingModel,
				embeddingDims, time.Duration(cfg.EmbeddingTimeout)*time.Millisecond)
		} else {
			embedder = embed.NewMock(embeddingDims)
		}
		sem := cache.NewPostgres(db, cfg.SemanticThreshold,
			time.Duration(cfg.CacheTTLSec)*time.Second,
			time.Duration(cfg.SemanticTimeoutMS)*time.Millisecond)
		handler.Semantic, handler.Embedder = sem, embedder
		go cleanSemanticCache(ctx, sem) // deletes old rows once an hour
		slog.Info("semantic cache on", "embedding", cfg.EmbeddingProvider, "threshold", cfg.SemanticThreshold)
	}
	slog.Info("cache", "enabled", cfg.CacheEnabled, "ttl_seconds", cfg.CacheTTLSec, "allow_temperature", cfg.CacheAllowTemp)

	// The usage pipeline: handlers put events in a Redis Stream, this consumer writes them to Postgres.
	// It has its own context, because at shutdown it must keep running until the HTTP server is done.
	consumerCtx, stopConsumer := context.WithCancel(context.Background())
	defer stopConsumer()
	consumerDone := make(chan struct{})
	if cfg.UsageEnabled {
		writer := usage.NewPGWriter(db)
		handler.Usage = usage.NewRecorder(usage.NewStream(rdb, int64(cfg.UsageStreamMaxLen)), writer,
			time.Duration(cfg.UsagePublishTimeout)*time.Millisecond)
		consumer := usage.NewConsumer(rdb, writer, usage.ConsumerConfig{
			Batch:     int64(cfg.UsageBatch),
			Block:     time.Duration(cfg.UsagePollMS) * time.Millisecond,
			Blocking:  cfg.UsageBlocking,
			ClaimIdle: time.Duration(cfg.UsageClaimIdleSec) * time.Second,
		})
		go func() {
			consumer.Run(consumerCtx, time.Duration(cfg.UsageDrainSec)*time.Second)
			close(consumerDone)
		}()
		slog.Info("usage pipeline on", "batch", cfg.UsageBatch, "blocking_read", cfg.UsageBlocking)
	} else {
		close(consumerDone)
		slog.Warn("usage pipeline is OFF: usage events are not recorded")
	}

	mode := "closed"
	if cfg.RateLimitFailOpen {
		mode = "open"
	}
	slog.Info("rate limiting on", "on_redis_down", mode)
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           api.NewRouter(handler, cfg.MaxBodyBytes),
		ReadHeaderTimeout: 5 * time.Second, // protects against slow-header attacks
	}

	serverErr := make(chan error, 1)
	go func() {
		slog.Info("gateway listening", "port", cfg.Port)
		serverErr <- srv.ListenAndServe()
	}()

	// 7. Wait for a stop signal or a server error.
	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		slog.Info("shutdown signal received, draining requests")
	}

	// Graceful shutdown, in this order:
	//  1. Stop accepting new requests and wait up to 15s for the running ones.
	//     Every handler puts its usage event in the stream BEFORE it returns, so after this step
	//     all events of finished requests are in Redis.
	//  2. Tell the consumer to stop reading. It then writes everything that is left to Postgres.
	//  3. Wait until it is done. Only then the connections are closed (the defers above).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	shutdownErr := srv.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		slog.Warn("some requests were still running at shutdown", "err", shutdownErr)
	}
	stopConsumer()
	<-consumerDone
	if shutdownErr != nil {
		return shutdownErr
	}
	slog.Info("gateway stopped cleanly")
	return nil
}

// embeddingDims is the size of our vectors. It must match vector(768) in the migration.
const embeddingDims = 768

// cleanSemanticCache removes expired rows from the semantic cache once an hour, until ctx ends.
func cleanSemanticCache(ctx context.Context, c *cache.Postgres) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := c.Cleanup(ctx)
			if err != nil {
				slog.Warn("semantic cache cleanup failed", "err", err)
			} else if n > 0 {
				slog.Info("semantic cache cleanup", "deleted_rows", n)
			}
		}
	}
}
