package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"taskflow/internal/config"
	"taskflow/internal/handler"
	"taskflow/internal/repository/postgres"
	"taskflow/internal/task"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("application stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := postgres.Open(ctx, cfg.DatabaseURL, cfg.DBTimeout, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()
	// Migrations are an explicit deployment step, never a side effect of startup.
	checkCtx, cancelCheck := context.WithTimeout(ctx, cfg.DBTimeout)
	_, err = pool.Exec(checkCtx, "SELECT id FROM tasks LIMIT 0")
	cancelCheck()
	if err != nil {
		return errors.New("task schema is unavailable; apply database migrations before starting the API")
	}
	service := task.NewService(postgres.New(pool, cfg.DBTimeout))
	server := &http.Server{
		Addr: cfg.HTTPAddr, Handler: handler.New(service, logger),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 16 * 1024,
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	logger.Info("HTTP server starting", "address", cfg.HTTPAddr)
	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	case <-ctx.Done():
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
		return fmt.Errorf("shutdown HTTP: %w", err)
	}
	if err := <-serverErrors; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP: %w", err)
	}
	logger.Info("HTTP server stopped")
	return nil
}
