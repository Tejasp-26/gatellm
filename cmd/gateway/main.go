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
	"gatellm/internal/config"
	"gatellm/internal/provider"
	"gatellm/internal/ratelimit"
	"gatellm/internal/store"
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
	providers := provider.Registry{
		"mock": provider.NewMock(time.Duration(cfg.MockLatency)*time.Millisecond, cfg.MockErrorPct),
	}
	if cfg.GroqAPIKey != "" {
		providers["groq"] = provider.NewGroq(cfg.GroqAPIKey)
	}
	if cfg.GeminiAPIKey != "" {
		providers["gemini"] = provider.NewGemini(cfg.GeminiAPIKey)
	}
	slog.Info("providers enabled", "names", providers.Names())

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
		Limiter: ratelimit.New(rdb, cfg.RateLimitFailOpen,
			time.Duration(cfg.RateLimitTimeoutMS)*time.Millisecond),
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

	// Graceful shutdown: stop accepting new requests, wait up to 15s for running ones.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	slog.Info("gateway stopped cleanly")
	return nil
}
