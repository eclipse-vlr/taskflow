package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"taskflow/internal/task"
)

type Repository struct {
	pool    *pgxpool.Pool
	timeout time.Duration
}

// Open verifies connectivity before returning the pool to its owner.
func Open(ctx context.Context, databaseURL string, timeout time.Duration, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		// A parser error may contain credentials from the connection string.
		return nil, errors.New("invalid database connection settings")
	}
	if timeout < time.Millisecond || maxConns < 1 {
		return nil, errors.New("invalid database timeout or pool limit")
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = 0
	cfg.MinIdleConns = 0
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams["application_name"] = "taskflow"
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(timeout.Milliseconds(), 10)
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = strconv.FormatInt(min(timeout.Milliseconds(), 2000), 10)
	cfg.ConnConfig.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelRequestContextWatcherHandler{Conn: conn, DeadlineDelay: time.Second}
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("create PostgreSQL connection pool failed")
	}
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, errors.New("connect to PostgreSQL failed")
	}
	return pool, nil
}

func New(pool *pgxpool.Pool, timeout time.Duration) *Repository {
	return &Repository{pool: pool, timeout: timeout}
}

func (r *Repository) Create(ctx context.Context, created task.Task) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	_, err := r.pool.Exec(ctx, `INSERT INTO tasks
		(id, type, payload, status, attempts, max_attempts, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		created.ID, created.Type, created.Payload, created.Status, created.Attempts,
		created.MaxAttempts, created.CreatedAt, created.UpdatedAt)
	return operationError(ctx, "insert task", err)
}

const columns = `t.id::text, t.type, t.payload, t.status, t.attempts, t.max_attempts,
	t.created_at, t.updated_at, t.started_at, t.finished_at, t.result, t.last_error,
	t.next_run_at, COALESCE(t.execution_id::text, ''), t.lease_expires_at`

func (r *Repository) Get(ctx context.Context, id string) (task.Task, error) {
	var uuid pgtype.UUID
	if !validID(id) || uuid.Scan(id) != nil {
		return task.Task{}, task.ErrNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	found, err := scanTask(r.pool.QueryRow(ctx, `SELECT `+columns+` FROM tasks t WHERE t.id = $1`, uuid))
	if errors.Is(err, pgx.ErrNoRows) {
		return task.Task{}, task.ErrNotFound
	}
	return found, operationError(ctx, "select task", err)
}

func (r *Repository) List(ctx context.Context, options task.ListOptions) ([]task.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	rows, err := r.pool.Query(ctx, `SELECT `+columns+` FROM tasks t ORDER BY t.created_at DESC, t.id DESC LIMIT $1 OFFSET $2`, options.Limit, options.Offset)
	if err != nil {
		return nil, operationError(ctx, "list tasks", err)
	}
	defer rows.Close()
	tasks := make([]task.Task, 0)
	for rows.Next() {
		found, err := scanTask(rows)
		if err != nil {
			return nil, operationError(ctx, "scan task", err)
		}
		tasks = append(tasks, found)
	}
	if err := rows.Err(); err != nil {
		return nil, operationError(ctx, "iterate tasks", err)
	}
	return tasks, nil
}

func scanTask(row pgx.Row) (task.Task, error) {
	var found task.Task
	var startedAt, finishedAt, nextRunAt, leaseExpiresAt pgtype.Timestamptz
	var lastError pgtype.Text
	var result []byte
	err := row.Scan(&found.ID, &found.Type, &found.Payload, &found.Status,
		&found.Attempts, &found.MaxAttempts, &found.CreatedAt, &found.UpdatedAt,
		&startedAt, &finishedAt, &result, &lastError, &nextRunAt, &found.ExecutionID, &leaseExpiresAt)
	if err != nil {
		return task.Task{}, err
	}
	if startedAt.Valid {
		found.StartedAt = startedAt.Time
	}
	if finishedAt.Valid {
		found.FinishedAt = finishedAt.Time
	}
	found.LastError = lastError.String
	if nextRunAt.Valid {
		found.NextRunAt = nextRunAt.Time
	}
	if leaseExpiresAt.Valid {
		found.LeaseExpiresAt = leaseExpiresAt.Time
	}
	if len(result) != 0 {
		if err := json.Unmarshal(result, &found.Result); err != nil {
			return task.Task{}, fmt.Errorf("decode task result: %w", err)
		}
	}
	return found, nil
}

// ClaimNext changes one ready task to running in a single database statement.
// SKIP LOCKED allows independent workers to claim different tasks.
func (r *Repository) ClaimNext(ctx context.Context) (task.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	claimed, err := scanTask(r.pool.QueryRow(ctx, `WITH candidate AS (
		SELECT id FROM tasks
		WHERE status = 'pending' AND attempts < max_attempts
		AND (next_run_at IS NULL OR next_run_at <= clock_timestamp())
		ORDER BY created_at, id
		FOR UPDATE SKIP LOCKED LIMIT 1
	)
	UPDATE tasks t SET status = 'running', attempts = t.attempts + 1,
		started_at = clock_timestamp(), finished_at = NULL, result = NULL,
		next_run_at = NULL, execution_id = gen_random_uuid(),
		lease_expires_at = clock_timestamp() + interval '90 seconds',
		updated_at = clock_timestamp()
	FROM candidate WHERE t.id = candidate.id RETURNING `+columns))
	if errors.Is(err, pgx.ErrNoRows) {
		return task.Task{}, task.ErrNoReady
	}
	return claimed, operationError(ctx, "claim task", err)
}

func (r *Repository) Finish(ctx context.Context, id string, attempt int, outcome task.Outcome) error {
	if (outcome.Status != task.StatusSucceeded && outcome.Status != task.StatusFailed && outcome.Status != task.StatusPending) ||
		(outcome.Status == task.StatusSucceeded && (outcome.Result.DurationMS < 1 || outcome.LastError != "")) ||
		(outcome.Status != task.StatusSucceeded && (outcome.LastError == "" || len(outcome.LastError) > 1024)) ||
		(outcome.Status == task.StatusPending && (outcome.RetryDelay < time.Second || outcome.RetryDelay > 30*time.Second)) {
		return task.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	var result any
	var lastError any
	if outcome.Status == task.StatusSucceeded {
		result = outcome.Result
	} else {
		lastError = outcome.LastError
	}
	updated, err := r.pool.Exec(ctx, `UPDATE tasks SET status = $3, result = $4,
		last_error = $5, finished_at = CASE WHEN $3 = 'pending' THEN NULL ELSE clock_timestamp() END,
		next_run_at = CASE WHEN $3 = 'pending' THEN clock_timestamp() + $7::bigint * interval '1 millisecond' ELSE NULL END,
		execution_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'running' AND attempts = $2 AND execution_id::text = $6
		AND lease_expires_at > clock_timestamp() AND ($3 <> 'pending' OR attempts < max_attempts)`,
		id, attempt, outcome.Status, result, lastError, outcome.ExecutionID, outcome.RetryDelay.Milliseconds())
	if err != nil {
		return operationError(ctx, "finish task", err)
	}
	if updated.RowsAffected() != 1 {
		return task.ErrConflict
	}
	return nil
}

func operationError(ctx context.Context, operation string, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return fmt.Errorf("%s: %w", operation, ctx.Err())
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) && (pgError.Code == "57014" || pgError.Code == "55P03") {
		return fmt.Errorf("%s: %w", operation, context.DeadlineExceeded)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
