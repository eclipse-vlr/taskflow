//go:build integration

package integration_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"taskflow/internal/repository/postgres"
	"taskflow/internal/scheduler"
	"taskflow/internal/task"
	"taskflow/internal/worker"
)

func TestConcurrentClaimsSkipLockedRows(t *testing.T) {
	db := newDatabase(t)
	pool, err := postgres.Open(t.Context(), db.url, 3*time.Second, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	repository := postgres.New(pool, 3*time.Second)
	const count = 20
	var lockedID string
	for i := range count {
		created := createTask(t, repository)
		if i == 0 {
			lockedID = created.ID
		}
	}
	future := createTask(t, repository)
	if _, err := pool.Exec(t.Context(), "UPDATE tasks SET next_run_at = now() + interval '1 hour' WHERE id = $1", future.ID); err != nil {
		t.Fatal(err)
	}
	blocker, err := db.admin.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = blocker.Rollback(ctx)
	}()
	if _, err := blocker.Exec(t.Context(), "SELECT id FROM "+pgx.Identifier{db.schema, "tasks"}.Sanitize()+" WHERE id = $1 FOR UPDATE", lockedID); err != nil {
		t.Fatal(err)
	}
	claimed := make(chan task.Task, count*2)
	failures := make(chan error, count*2)
	start := make(chan struct{})
	var callers sync.WaitGroup
	for range count * 2 {
		callers.Go(func() {
			<-start
			current, err := repository.ClaimNext(t.Context())
			if errors.Is(err, task.ErrNoReady) {
				return
			}
			if err != nil {
				failures <- err
				return
			}
			claimed <- current
		})
	}
	close(start)
	callers.Wait()
	close(claimed)
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	seen := make(map[string]task.Task)
	for current := range claimed {
		if _, exists := seen[current.ID]; exists || current.ID == lockedID || current.ID == future.ID {
			t.Errorf("unexpected or duplicate claim: %+v", current)
		}
		if current.Status != task.StatusRunning || current.Attempts != 1 || current.StartedAt.IsZero() || !current.FinishedAt.IsZero() {
			t.Errorf("invalid running state: %+v", current)
		}
		seen[current.ID] = current
	}
	if len(seen) != count-1 {
		t.Fatalf("unique claims=%d, want=%d", len(seen), count-1)
	}
	if err := blocker.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	current, err := repository.ClaimNext(t.Context())
	if err != nil || current.ID != lockedID {
		t.Fatalf("claim after unlock: %+v, %v", current, err)
	}
	if _, err := repository.ClaimNext(t.Context()); !errors.Is(err, task.ErrNoReady) {
		t.Fatalf("running or future task was claimed: %v", err)
	}
	success := task.Outcome{Status: task.StatusSucceeded, Result: task.SleepResult{DurationMS: 1000}, ExecutionID: current.ExecutionID}
	if err := repository.Finish(t.Context(), current.ID, current.Attempts+1, success); !errors.Is(err, task.ErrConflict) {
		t.Fatalf("stale completion returned %v", err)
	}
	if err := repository.Finish(t.Context(), current.ID, current.Attempts, success); err != nil {
		t.Fatal(err)
	}
	if err := repository.Finish(t.Context(), current.ID, current.Attempts, success); !errors.Is(err, task.ErrConflict) {
		t.Fatalf("duplicate completion returned %v", err)
	}
	stored, err := repository.Get(t.Context(), current.ID)
	if err != nil || stored.Status != task.StatusSucceeded || stored.Result != success.Result || stored.FinishedAt.IsZero() || stored.FinishedAt.Before(stored.StartedAt) {
		t.Fatalf("completion not persisted: %+v, %v", stored, err)
	}
}

func TestWorkerPipelinePersistsOutcomes(t *testing.T) {
	db := newDatabase(t)
	repository := postgres.New(db.pool, time.Second)
	service := task.NewService(repository)
	for range 6 {
		if _, err := service.Create(t.Context(), task.CreateInput{Type: "sleep", Payload: task.SleepPayload{DurationMS: 20}}); err != nil {
			t.Fatal(err)
		}
	}
	broken := createTask(t, repository)
	if _, err := db.pool.Exec(t.Context(), "UPDATE tasks SET type = 'unsupported', max_attempts = 1 WHERE id = $1", broken.ID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := runWorkers(t, ctx, repository, 3)
	waitFor(t, func() bool {
		var count int
		err := db.pool.QueryRow(t.Context(), "SELECT count(*) FROM tasks WHERE status IN ('succeeded','failed')").Scan(&count)
		return err == nil && count == 7
	}, "all worker outcomes")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	stored, err := repository.Get(t.Context(), broken.ID)
	if err != nil || stored.Status != task.StatusFailed || stored.LastError != "unsupported task type" || stored.Attempts != 1 || stored.FinishedAt.IsZero() {
		t.Fatalf("failed task: %+v, %v", stored, err)
	}
	var successes int
	if err := db.pool.QueryRow(t.Context(), "SELECT count(*) FROM tasks WHERE status = 'succeeded' AND attempts = 1 AND result->>'duration_ms' = '20' AND started_at IS NOT NULL AND finished_at IS NOT NULL").Scan(&successes); err != nil || successes != 6 {
		t.Fatalf("persisted successes=%d error=%v", successes, err)
	}
}

func TestWorkerShutdownPersistsInterruptedTask(t *testing.T) {
	db := newDatabase(t)
	repository := postgres.New(db.pool, time.Second)
	created, err := task.NewService(repository).Create(t.Context(), task.CreateInput{Type: "sleep", Payload: task.SleepPayload{DurationMS: 60000}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := runWorkers(t, ctx, repository, 1)
	waitFor(t, func() bool {
		current, err := repository.Get(t.Context(), created.ID)
		return err == nil && current.Status == task.StatusRunning
	}, "task running before shutdown")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	stored, err := repository.Get(t.Context(), created.ID)
	if err != nil || stored.Status != task.StatusPending || stored.LastError != context.Canceled.Error() || stored.Attempts != 1 || !stored.FinishedAt.IsZero() || stored.NextRunAt.IsZero() || stored.ExecutionID != "" {
		t.Fatalf("interrupted task: %+v, %v", stored, err)
	}
}

func runWorkers(t *testing.T, ctx context.Context, repository *postgres.Repository, count int) <-chan error {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	source, err := scheduler.New(repository, 10*time.Millisecond, logger)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := worker.New(count, source, repository, worker.Sleep, 2*time.Second, logger)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- pool.Run(ctx) }()
	return done
}
