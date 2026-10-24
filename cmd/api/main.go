package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"taskflow/internal/config"
	"taskflow/internal/handler"
	"taskflow/internal/repository/postgres"
	"taskflow/internal/scheduler"
	"taskflow/internal/task"
	"taskflow/internal/worker"
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
	repository := postgres.New(pool, cfg.DBTimeout)
	service := task.NewService(repository)
	source, err := scheduler.New(repository, cfg.PollInterval, logger)
	if err != nil {
		return err
	}
	workers, err := worker.New(cfg.WorkerCount, source, repository, worker.Sleep, cfg.DBTimeout+time.Second, logger)
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr: cfg.HTTPAddr, Handler: handler.New(service, logger),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 16 * 1024,
	}
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen HTTP: %w", err)
	}
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	// Each runner sends exactly once. The owner drains both results on shutdown.
	results := make(chan error, 2)
	go func() { results <- server.Serve(listener) }()
	go func() { results <- workers.Run(workerCtx) }()
	logger.Info("HTTP server and workers started", "address", cfg.HTTPAddr, "workers", cfg.WorkerCount)
	completed := 0
	var runErr error
	select {
	case err := <-results:
		completed++
		if !errors.Is(err, http.ErrServerClosed) {
			runErr = err
		}
	case <-ctx.Done():
	}
	cancelWorkers()
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
		runErr = errors.Join(runErr, fmt.Errorf("shutdown HTTP: %w", err))
	}
	for completed < 2 {
		err := <-results
		completed++
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			runErr = errors.Join(runErr, err)
		}
	}
	logger.Info("HTTP server and workers stopped")
	return runErr
}
