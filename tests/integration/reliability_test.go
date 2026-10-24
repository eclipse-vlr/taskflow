//go:build integration

package integration_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"taskflow/internal/repository/postgres"
	"taskflow/internal/scheduler"
	"taskflow/internal/task"
	"taskflow/internal/worker"
)

func TestLeaseMigrationUpgradesInterruptedLegacyTask(t *testing.T) {
	db := newDatabase(t)
	repository := postgres.New(db.pool, time.Second)
	created := createTask(t, repository)
	applyLeaseMigration := func(direction string) {
		t.Helper()
		sql, err := os.ReadFile(filepath.Join("..", "..", "migrations", "000002_execution_leases."+direction+".sql"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.pool.Exec(t.Context(), string(sql)); err != nil {
			t.Fatal(err)
		}
	}
	applyLeaseMigration("down")
	if _, err := db.pool.Exec(t.Context(), "UPDATE tasks SET status = 'running', attempts = 1 WHERE id = $1", created.ID); err != nil {
		t.Fatal(err)
	}
	applyLeaseMigration("up")
	if count, err := repository.RecoverExpired(t.Context()); err != nil || count != 1 {
		t.Fatalf("legacy recovery: %d, %v", count, err)
	}
	stored, err := repository.Get(t.Context(), created.ID)
	if err != nil || stored.Status != task.StatusPending || stored.Attempts != 1 {
		t.Fatalf("legacy task lost: %+v, %v", stored, err)
	}
	applyLeaseMigration("down")
	applyLeaseMigration("up")
	if _, err := repository.Get(t.Context(), created.ID); err != nil {
		t.Fatalf("task lost after lease migration round trip: %v", err)
	}
}

func TestCancelAndClaimAreMutuallyExclusive(t *testing.T) {
	db := newDatabase(t)
	pool, err := postgres.Open(t.Context(), db.url, time.Second, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	repository := postgres.New(pool, time.Second)
	for range 10 {
		created := createTask(t, repository)
		start := make(chan struct{})
		var claimed, cancelled task.Task
		var claimErr, cancelErr error
		var callers sync.WaitGroup
		callers.Go(func() { <-start; claimed, claimErr = repository.ClaimNext(t.Context()) })
		callers.Go(func() { <-start; cancelled, cancelErr = repository.Cancel(t.Context(), created.ID) })
		close(start)
		callers.Wait()
		switch {
		case claimErr == nil:
			if claimed.ID != created.ID || !errors.Is(cancelErr, task.ErrConflict) {
				t.Fatalf("claim and cancel both succeeded: %+v, %v", cancelled, cancelErr)
			}
			if err := repository.Finish(t.Context(), claimed.ID, claimed.Attempts, task.Outcome{Status: task.StatusSucceeded, Result: task.SleepResult{DurationMS: 1}, ExecutionID: claimed.ExecutionID}); err != nil {
				t.Fatal(err)
			}
		case cancelErr == nil:
			if cancelled.Status != task.StatusCancelled || !errors.Is(claimErr, task.ErrNoReady) {
				t.Fatalf("cancel and claim both succeeded: %+v, %v", claimed, claimErr)
			}
		default:
			t.Fatalf("neither transition succeeded: %v, %v", claimErr, cancelErr)
		}
	}
}

func TestConcurrentManualRetryAndInvalidCommands(t *testing.T) {
	db := newDatabase(t)
	repository := postgres.New(db.pool, time.Second)
	created := createTask(t, repository)
	if _, err := db.pool.Exec(t.Context(), "UPDATE tasks SET status = 'failed', attempts = max_attempts, last_error = 'failed', finished_at = clock_timestamp() WHERE id = $1", created.ID); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	start := make(chan struct{})
	var callers sync.WaitGroup
	for range 2 {
		callers.Go(func() { <-start; _, err := repository.Retry(t.Context(), created.ID); results <- err })
	}
	close(start)
	callers.Wait()
	close(results)
	succeeded, conflicted := 0, 0
	for err := range results {
		if err == nil {
			succeeded++
		} else if errors.Is(err, task.ErrConflict) {
			conflicted++
		} else {
			t.Fatal(err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("retry successes=%d conflicts=%d", succeeded, conflicted)
	}
	for _, command := range []func(context.Context, string) (task.Task, error){repository.Retry, repository.Cancel} {
		for _, id := range []string{"missing", "00000000-0000-4000-8000-000000000000", strings.ReplaceAll(created.ID, "-", "z")} {
			if _, err := command(t.Context(), id); !errors.Is(err, task.ErrNotFound) {
				t.Fatalf("invalid/missing command ID %q: %v", id, err)
			}
		}
	}
}

func TestRetryBackoffAndExhaustion(t *testing.T) {
	db := newDatabase(t)
	repository := postgres.New(db.pool, time.Second)
	created := createTask(t, repository)
	for attempt := 1; attempt <= created.MaxAttempts; attempt++ {
		current, err := repository.ClaimNext(t.Context())
		if err != nil || current.ID != created.ID || current.Attempts != attempt || current.ExecutionID == "" {
			t.Fatalf("claim %d: %+v, %v", attempt, current, err)
		}
		outcome := task.Outcome{Status: task.StatusFailed, LastError: "temporary failure", ExecutionID: current.ExecutionID}
		if attempt < created.MaxAttempts {
			outcome.Status, outcome.RetryDelay = task.StatusPending, task.RetryDelay(attempt)
		}
		if err := repository.Finish(t.Context(), current.ID, attempt, outcome); err != nil {
			t.Fatal(err)
		}
		stored, err := repository.Get(t.Context(), created.ID)
		if err != nil || stored.Status != outcome.Status || stored.ExecutionID != "" || !stored.LeaseExpiresAt.IsZero() || stored.LastError != outcome.LastError {
			t.Fatalf("finish %d: %+v, %v", attempt, stored, err)
		}
		if _, err := repository.ClaimNext(t.Context()); !errors.Is(err, task.ErrNoReady) {
			t.Fatalf("task claimed before backoff or after exhaustion: %v", err)
		}
		if outcome.Status == task.StatusPending {
			if stored.NextRunAt.Sub(stored.UpdatedAt) < outcome.RetryDelay-10*time.Millisecond || !stored.FinishedAt.IsZero() {
				t.Fatalf("missing backoff: %+v", stored)
			}
			if _, err := db.pool.Exec(t.Context(), "UPDATE tasks SET next_run_at = clock_timestamp() - interval '1 second' WHERE id = $1", created.ID); err != nil {
				t.Fatal(err)
			}
		} else if stored.FinishedAt.IsZero() || !stored.NextRunAt.IsZero() {
			t.Fatalf("invalid terminal state: %+v", stored)
		}
	}
}

func TestRecoveryPreservesLiveLeasesAndRejectsOldExecution(t *testing.T) {
	db := newDatabase(t)
	repository := postgres.New(db.pool, time.Second)
	created := createTask(t, repository)
	old, err := repository.ClaimNext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if count, err := repository.RecoverExpired(t.Context()); err != nil || count != 0 {
		t.Fatalf("active execution recovered: %d, %v", count, err)
	}
	if _, err := db.pool.Exec(t.Context(), "UPDATE tasks SET lease_expires_at = clock_timestamp() - interval '1 second', max_attempts = 1 WHERE id = $1", created.ID); err != nil {
		t.Fatal(err)
	}
	success := task.Outcome{Status: task.StatusSucceeded, Result: task.SleepResult{DurationMS: 1000}, ExecutionID: old.ExecutionID}
	if err := repository.Finish(t.Context(), old.ID, old.Attempts, success); !errors.Is(err, task.ErrConflict) {
		t.Fatalf("expired execution completed: %v", err)
	}
	// A new pool represents a restarted application; no live owner is reset.
	reopened, err := postgres.Open(t.Context(), db.url, time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	repository = postgres.New(reopened, time.Second)
	if count, err := repository.RecoverExpired(t.Context()); err != nil || count != 1 {
		t.Fatalf("recover exhausted execution: %d, %v", count, err)
	}
	failed, err := repository.Get(t.Context(), old.ID)
	if err != nil || failed.Status != task.StatusFailed || failed.FinishedAt.IsZero() {
		t.Fatalf("exhausted recovery: %+v, %v", failed, err)
	}
	reset, err := repository.Retry(t.Context(), old.ID)
	if err != nil || reset.Status != task.StatusPending || reset.Attempts != 0 || reset.LastError != "" || !reset.StartedAt.IsZero() || !reset.FinishedAt.IsZero() {
		t.Fatalf("manual retry: %+v, %v", reset, err)
	}
	current, err := repository.ClaimNext(t.Context())
	if err != nil || current.Attempts != old.Attempts || current.ExecutionID == old.ExecutionID {
		t.Fatalf("new execution: %+v, %v", current, err)
	}
	if err := repository.Finish(t.Context(), old.ID, old.Attempts, success); !errors.Is(err, task.ErrConflict) {
		t.Fatalf("old execution overwrote a new attempt with the same number: %v", err)
	}
	success.ExecutionID = current.ExecutionID
	if err := repository.Finish(t.Context(), current.ID, current.Attempts, success); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryRequeuesInterruptedAndFailsExhaustedPending(t *testing.T) {
	db := newDatabase(t)
	repository := postgres.New(db.pool, time.Second)
	created := createTask(t, repository)
	if _, err := repository.ClaimNext(t.Context()); err != nil {
		t.Fatal(err)
	}
	stranded := createTask(t, repository)
	if _, err := db.pool.Exec(t.Context(), "UPDATE tasks SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE id = $1", created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.pool.Exec(t.Context(), "UPDATE tasks SET attempts = max_attempts WHERE id = $1", stranded.ID); err != nil {
		t.Fatal(err)
	}
	if count, err := repository.RecoverExpired(t.Context()); err != nil || count != 2 {
		t.Fatalf("recover: %d, %v", count, err)
	}
	current, err := repository.ClaimNext(t.Context())
	if err != nil || current.ID != created.ID || current.Attempts != 2 {
		t.Fatalf("interrupted task not requeued: %+v, %v", current, err)
	}
	stored, err := repository.Get(t.Context(), stranded.ID)
	if err != nil || stored.Status != task.StatusFailed || stored.FinishedAt.IsZero() {
		t.Fatalf("exhausted pending task stranded: %+v, %v", stored, err)
	}
}

func TestWorkerRetriesTransientAndPermanentErrors(t *testing.T) {
	db := newDatabase(t)
	repository := postgres.New(db.pool, time.Second)
	transient := createTask(t, repository)
	permanent := createTask(t, repository)
	if _, err := db.pool.Exec(t.Context(), "UPDATE tasks SET max_attempts = 2"); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	source, err := scheduler.New(repository, 10*time.Millisecond, logger)
	if err != nil {
		t.Fatal(err)
	}
	execute := func(_ context.Context, current task.Task) (task.SleepResult, error) {
		if current.ID == permanent.ID || current.Attempts == 1 {
			return task.SleepResult{}, errors.New("execution failed")
		}
		return task.SleepResult{DurationMS: 1}, nil
	}
	pool, err := worker.New(2, source, repository, execute, time.Second, logger)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pool.Run(ctx) }()
	// Join even if an assertion fails, before database cleanup runs.
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	waitFor(t, func() bool {
		var count int
		err := db.pool.QueryRow(t.Context(), "SELECT count(*) FROM tasks WHERE status IN ('succeeded', 'failed')").Scan(&count)
		return err == nil && count == 2
	}, "bounded retry pipeline")
	for _, expected := range []struct {
		id     string
		status task.Status
	}{{transient.ID, task.StatusSucceeded}, {permanent.ID, task.StatusFailed}} {
		stored, err := repository.Get(t.Context(), expected.id)
		if err != nil || stored.Status != expected.status || stored.Attempts != 2 || stored.FinishedAt.IsZero() {
			t.Fatalf("retry pipeline: %+v, %v", stored, err)
		}
	}
}
