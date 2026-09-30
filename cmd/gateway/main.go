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

	// 5. Start the HTTP server.
	handler := &api.Handler{DB: db, Redis: rdb}
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           api.NewRouter(handler),
		ReadHeaderTimeout: 5 * time.Second, // protects against slow-header attacks
	}

	serverErr := make(chan error, 1)
	go func() {
		slog.Info("gateway listening", "port", cfg.Port)
		serverErr <- srv.ListenAndServe()
	}()

	// 6. Wait for a stop signal or a server error.
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
