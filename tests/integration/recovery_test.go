//go:build integration

package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"taskflow/internal/repository/postgres"
	"taskflow/internal/task"
)

func TestRecoverySkipsLockedTasks(t *testing.T) {
	db := newDatabase(t)
	repository := postgres.New(db.pool, 250*time.Millisecond)
	locked := createTask(t, repository)
	ready := createTask(t, repository)
	for range 2 {
		if _, err := repository.ClaimNext(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.pool.Exec(t.Context(), "UPDATE tasks SET lease_expires_at = clock_timestamp() - interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	live := createTask(t, repository)
	claimedLive, err := repository.ClaimNext(t.Context())
	if err != nil || claimedLive.ID != live.ID {
		t.Fatalf("claim live task: %+v, %v", claimedLive, err)
	}
	exhausted := createTask(t, repository)
	if _, err := db.pool.Exec(t.Context(), "UPDATE tasks SET attempts = max_attempts WHERE id = $1", exhausted.ID); err != nil {
		t.Fatal(err)
	}
	blocker, err := db.admin.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = blocker.Rollback(ctx)
	})
	if _, err := blocker.Exec(t.Context(), "SELECT id FROM "+pgx.Identifier{db.schema, "tasks"}.Sanitize()+" WHERE id = $1 FOR UPDATE", locked.ID); err != nil {
		t.Fatal(err)
	}
	if count, err := repository.RecoverExpired(t.Context()); err != nil || count != 2 {
		t.Fatalf("recover unlocked tasks: count=%d, error=%v", count, err)
	}
	for _, expected := range []struct {
		id     string
		status task.Status
	}{{locked.ID, task.StatusRunning}, {ready.ID, task.StatusPending}, {exhausted.ID, task.StatusFailed}} {
		stored, err := repository.Get(t.Context(), expected.id)
		if err != nil || stored.Status != expected.status {
			t.Fatalf("task %s: %+v, %v", expected.id, stored, err)
		}
	}
	if stored, err := repository.Get(t.Context(), live.ID); err != nil || stored != claimedLive {
		t.Fatalf("live execution changed: %+v, %v", stored, err)
	}
	if err := blocker.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if count, err := repository.RecoverExpired(t.Context()); err != nil || count != 1 {
		t.Fatalf("recover after unlock: count=%d, error=%v", count, err)
	}
	if stored, err := repository.Get(t.Context(), locked.ID); err != nil || stored.Status != task.StatusPending {
		t.Fatalf("unlocked task not recovered: %+v, %v", stored, err)
	}
}

func TestRecoveryBoundsEachBatch(t *testing.T) {
	db := newDatabase(t)
	seedExhaustedTasks(t, db, 205)
	repository := postgres.New(db.pool, time.Second)
	for _, want := range []int64{100, 100, 5, 0} {
		if count, err := repository.RecoverExpired(t.Context()); err != nil || count != want {
			t.Fatalf("recovery batch: count=%d, want=%d, error=%v", count, want, err)
		}
	}
	assertExhaustedTasksRecovered(t, db, 205)
}

func TestConcurrentRecoveriesDoNotProcessTasksTwice(t *testing.T) {
	db := newDatabase(t)
	seedExhaustedTasks(t, db, 205)
	pool, err := postgres.Open(t.Context(), db.url, time.Second, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	repository := postgres.New(pool, time.Second)
	type result struct {
		count int64
		err   error
	}
	results := make(chan result, 4)
	start := make(chan struct{})
	var callers sync.WaitGroup
	for range 4 {
		callers.Go(func() {
			<-start
			count, err := repository.RecoverExpired(t.Context())
			results <- result{count, err}
		})
	}
	close(start)
	callers.Wait()
	close(results)
	var total int64
	for result := range results {
		if result.err != nil || result.count < 0 || result.count > 100 {
			t.Fatalf("concurrent recovery: %+v", result)
		}
		total += result.count
	}
	if total != 205 {
		t.Fatalf("total recovered=%d, want=205", total)
	}
	assertExhaustedTasksRecovered(t, db, 205)
}

func seedExhaustedTasks(t *testing.T, db *database, count int) {
	t.Helper()
	_, err := db.pool.Exec(t.Context(), `INSERT INTO tasks (id, type, payload, status, attempts, max_attempts)
		SELECT gen_random_uuid(), 'sleep', '{"duration_ms":1}'::jsonb, 'pending', 1, 1
		FROM generate_series(1, $1::integer)`, count)
	if err != nil {
		t.Fatal(err)
	}
}

func assertExhaustedTasksRecovered(t *testing.T, db *database, want int) {
	t.Helper()
	var count int
	err := db.pool.QueryRow(t.Context(), `SELECT count(*) FROM tasks WHERE status = 'failed'
		AND finished_at IS NOT NULL AND execution_id IS NULL AND lease_expires_at IS NULL`).Scan(&count)
	if err != nil || count != want {
		t.Fatalf("recovered terminal tasks=%d, want=%d, error=%v", count, want, err)
	}
}
