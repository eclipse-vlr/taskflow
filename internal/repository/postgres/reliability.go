package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"taskflow/internal/task"
)

func validID(id string) bool {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return false
	}
	var uuid pgtype.UUID
	return uuid.Scan(id) == nil
}

// RecoverExpired handles at most 100 tasks without waiting for locked rows.
// Row locks serialize recovery with Finish and other recovery processes.
// Live leases are never reclaimed; skipped tasks wait for the next poll.
func (r *Repository) RecoverExpired(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	updated, err := r.pool.Exec(ctx, `WITH candidates AS (
		SELECT id FROM tasks
		WHERE (status = 'running' AND lease_expires_at <= clock_timestamp())
		OR (status = 'pending' AND attempts >= max_attempts)
		ORDER BY id
		FOR UPDATE SKIP LOCKED LIMIT 100
	)
	UPDATE tasks SET
		status = CASE WHEN attempts < max_attempts THEN 'pending' ELSE 'failed' END,
		next_run_at = CASE WHEN attempts < max_attempts THEN clock_timestamp() ELSE NULL END,
		finished_at = CASE WHEN attempts < max_attempts THEN NULL ELSE clock_timestamp() END,
		last_error = 'execution interrupted; attempt budget exhausted or lease expired',
		result = NULL, execution_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
		WHERE id IN (SELECT id FROM candidates)`)
	return updated.RowsAffected(), operationError(ctx, "recover tasks", err)
}

func (r *Repository) Retry(ctx context.Context, id string) (task.Task, error) {
	return r.transition(ctx, id, `UPDATE tasks t SET status = 'pending', attempts = 0,
		next_run_at = NULL, started_at = NULL, finished_at = NULL, result = NULL,
		last_error = NULL, execution_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'failed' RETURNING `+columns)
}

func (r *Repository) Cancel(ctx context.Context, id string) (task.Task, error) {
	return r.transition(ctx, id, `UPDATE tasks t SET status = 'cancelled',
		next_run_at = NULL, finished_at = clock_timestamp(), last_error = NULL, updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'pending' RETURNING `+columns)
}

func (r *Repository) transition(ctx context.Context, id, query string) (task.Task, error) {
	if !validID(id) {
		return task.Task{}, task.ErrNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	updated, err := scanTask(r.pool.QueryRow(ctx, query, id))
	if errors.Is(err, pgx.ErrNoRows) {
		// State changes are atomic; this read only distinguishes 404 from 409.
		if _, getErr := r.Get(ctx, id); getErr != nil {
			return task.Task{}, getErr
		}
		return task.Task{}, task.ErrConflict
	}
	return updated, operationError(ctx, "change task state", err)
}
