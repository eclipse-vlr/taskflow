package postgres

import (
	"context"
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

const columns = `id::text, type, payload, status, attempts, max_attempts, created_at, updated_at`

func (r *Repository) Get(ctx context.Context, id string) (task.Task, error) {
	if len(id) != 36 {
		return task.Task{}, task.ErrNotFound
	}
	var uuid pgtype.UUID
	if err := uuid.Scan(id); err != nil {
		return task.Task{}, task.ErrNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	found, err := scanTask(r.pool.QueryRow(ctx, `SELECT `+columns+` FROM tasks WHERE id = $1`, uuid))
	if errors.Is(err, pgx.ErrNoRows) {
		return task.Task{}, task.ErrNotFound
	}
	return found, operationError(ctx, "select task", err)
}

func (r *Repository) List(ctx context.Context, options task.ListOptions) ([]task.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	rows, err := r.pool.Query(ctx, `SELECT `+columns+` FROM tasks ORDER BY created_at DESC, id DESC LIMIT $1 OFFSET $2`, options.Limit, options.Offset)
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
	err := row.Scan(&found.ID, &found.Type, &found.Payload, &found.Status,
		&found.Attempts, &found.MaxAttempts, &found.CreatedAt, &found.UpdatedAt)
	return found, err
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
