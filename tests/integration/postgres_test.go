//go:build integration

package integration_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"taskflow/internal/repository/postgres"
	"taskflow/internal/task"
)

type database struct {
	url    string
	schema string
	pool   *pgxpool.Pool
	admin  *pgxpool.Pool
}

func newDatabase(t *testing.T) *database {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("TEST_DATABASE_URL is required for integration tests; use a disposable PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal("parse test database configuration")
	}
	t.Cleanup(admin.Close)
	if err := admin.Ping(ctx); err != nil {
		t.Fatal("connect to test database")
	}
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("taskflow_test_%x", random)
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal("invalid TEST_DATABASE_URL")
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	scopedURL := parsed.String()
	conn, err := pgx.Connect(ctx, scopedURL)
	if err != nil {
		t.Fatal("connect to isolated test schema")
	}
	migration := migrationSQL(t, "up")
	_, migrationErr := conn.Exec(ctx, migration)
	_ = conn.Close(ctx)
	if migrationErr != nil {
		t.Fatalf("apply migration: %v", migrationErr)
	}
	pool, err := postgres.Open(ctx, scopedURL, 3*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return &database{url: scopedURL, schema: schema, pool: pool, admin: admin}
}

func migrationSQL(t *testing.T, direction string) string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*."+direction+".sql"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no migration files")
	}
	slices.Sort(paths)
	if direction == "down" {
		slices.Reverse(paths)
	}
	var sql strings.Builder
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sql.Write(data)
		sql.WriteByte('\n')
	}
	return sql.String()
}

func createTask(t *testing.T, repository task.Repository) task.Task {
	t.Helper()
	created, err := task.NewService(repository).Create(t.Context(), task.CreateInput{
		Type: "sleep", Payload: task.SleepPayload{DurationMS: 1000},
	})
	if err != nil {
		t.Fatal(err)
	}
	return created
}

func TestPersistenceAcrossPools(t *testing.T) {
	db := newDatabase(t)
	repository := postgres.New(db.pool, time.Second)
	created := createTask(t, repository)
	db.pool.Close()
	reopened, err := postgres.Open(t.Context(), db.url, time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	found, err := postgres.New(reopened, time.Second).Get(t.Context(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if found.ID != created.ID || found.Type != created.Type || found.Payload != created.Payload || found.Status != task.StatusPending || found.Attempts != 0 || found.MaxAttempts != 3 || !found.CreatedAt.Equal(created.CreatedAt) || !found.UpdatedAt.Equal(created.UpdatedAt) {
		t.Fatalf("persisted task differs: got %+v, want %+v", found, created)
	}
	for _, id := range []string{"missing", "'; DROP TABLE tasks; --", "00000000-0000-4000-8000-000000000000"} {
		if _, err := postgres.New(reopened, time.Second).Get(t.Context(), id); !errors.Is(err, task.ErrNotFound) {
			t.Errorf("get %q: %v", id, err)
		}
	}
}

func TestGetUUIDValidation(t *testing.T) {
	db := newDatabase(t)
	repository := postgres.New(db.pool, time.Second)
	created := createTask(t, repository)
	for _, id := range []string{created.ID, strings.ToUpper(created.ID)} {
		found, err := repository.Get(t.Context(), id)
		if err != nil || found.ID != created.ID {
			t.Fatalf("get valid UUID %q = %q, %v", id, found.ID, err)
		}
	}
	type invalidID struct {
		name string
		id   string
	}
	invalidIDs := []invalidID{
		{"missing separators", strings.ReplaceAll(created.ID, "-", "")},
		{"all separators replaced", strings.ReplaceAll(created.ID, "-", "z")},
		{"invalid hex", "z" + created.ID[1:]},
		{"extra character", created.ID + "0"},
	}
	for _, position := range []int{8, 13, 18, 23} {
		invalidIDs = append(invalidIDs, invalidID{
			fmt.Sprintf("separator at %d", position), created.ID[:position] + "z" + created.ID[position+1:],
		})
	}
	for _, test := range invalidIDs {
		t.Run(test.name, func(t *testing.T) {
			if _, err := repository.Get(t.Context(), test.id); !errors.Is(err, task.ErrNotFound) {
				t.Fatalf("get malformed UUID %q: got %v, want ErrNotFound", test.id, err)
			}
		})
	}
}

func TestListAndConstraints(t *testing.T) {
	db := newDatabase(t)
	repository := postgres.New(db.pool, time.Second)
	empty, err := repository.List(t.Context(), task.ListOptions{Limit: 20})
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("empty list = %v, %v", empty, err)
	}
	var ids []string
	for range 3 {
		ids = append(ids, createTask(t, repository).ID)
	}
	if _, err := db.pool.Exec(t.Context(), "UPDATE tasks SET created_at = '2026-10-23T00:00:00Z'"); err != nil {
		t.Fatal(err)
	}
	slices.Sort(ids)
	slices.Reverse(ids)
	page, err := repository.List(t.Context(), task.ListOptions{Limit: 2, Offset: 1})
	if err != nil || len(page) != 2 || page[0].ID != ids[1] || page[1].ID != ids[2] {
		t.Fatalf("page = %+v, error = %v, expected IDs = %v", page, err, ids[1:])
	}
	for _, sql := range []string{
		"UPDATE tasks SET status = 'unknown'",
		"UPDATE tasks SET attempts = -1",
		"UPDATE tasks SET max_attempts = 0",
		"UPDATE tasks SET payload = 'null'::jsonb",
		"UPDATE tasks SET status = 'running'",
		"UPDATE tasks SET execution_id = gen_random_uuid()",
		"UPDATE tasks SET lease_expires_at = clock_timestamp()",
	} {
		_, err := db.pool.Exec(t.Context(), sql)
		var pgError *pgconn.PgError
		if !errors.As(err, &pgError) || pgError.Code != "23514" {
			t.Errorf("constraint error = %v for %s", err, sql)
		}
	}
	_, err = db.pool.Exec(t.Context(), migrationSQL(t, "down"))
	if err != nil {
		t.Fatalf("down migration: %v", err)
	}
	_, err = db.pool.Exec(t.Context(), migrationSQL(t, "up"))
	if err != nil {
		t.Fatalf("up after down: %v", err)
	}
}

func TestCancellationStopsSQL(t *testing.T) {
	db := newDatabase(t)
	repository := postgres.New(db.pool, 3*time.Second)
	created := createTask(t, repository)
	conn, err := db.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	pid := conn.Conn().PgConn().PID()
	conn.Release()
	blocker, err := db.admin.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = blocker.Rollback(ctx)
	}()
	if _, err := blocker.Exec(t.Context(), "LOCK TABLE "+pgx.Identifier{db.schema, "tasks"}.Sanitize()+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := repository.Get(ctx, created.ID)
		result <- err
	}()
	waitFor(t, func() bool {
		var waiting bool
		err := db.admin.QueryRow(t.Context(), "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND state = 'active' AND wait_event_type = 'Lock')", pid).Scan(&waiting)
		return err == nil && waiting
	}, "query waiting for the table lock")
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled query returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled query did not return")
	}
	waitFor(t, func() bool {
		var active bool
		err := db.admin.QueryRow(t.Context(), "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND state = 'active')", pid).Scan(&active)
		return err == nil && !active
	}, "no active SQL after cancellation while the table remains locked")
	_, err = postgres.New(db.pool, 100*time.Millisecond).Get(t.Context(), created.ID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("repository timeout returned %v", err)
	}
}

func TestConnectionErrorsHideCredentials(t *testing.T) {
	for _, databaseURL := range []string{"postgres://user:secret@%/bad", "postgres://user:secret@127.0.0.1:1/taskflow?sslmode=disable"} {
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		pool, err := postgres.Open(ctx, databaseURL, 100*time.Millisecond, 1)
		cancel()
		if pool != nil {
			pool.Close()
		}
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("unsafe connection error: %v", err)
		}
	}
}

func waitFor(t *testing.T, condition func() bool, description string) {
	t.Helper()
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-timeout.C:
			t.Fatalf("timed out waiting for %s", description)
		case <-tick.C:
		}
	}
}
