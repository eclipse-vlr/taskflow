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

// RecoverExpired never reclaims a running task while its execution lease is live.
// Conditional updates serialize with Finish and with other recovery processes.
func (r *Repository) RecoverExpired(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	updated, err := r.pool.Exec(ctx, `UPDATE tasks SET
		status = CASE WHEN attempts < max_attempts THEN 'pending' ELSE 'failed' END,
		next_run_at = CASE WHEN attempts < max_attempts THEN clock_timestamp() ELSE NULL END,
		finished_at = CASE WHEN attempts < max_attempts THEN NULL ELSE clock_timestamp() END,
		last_error = 'execution interrupted; attempt budget exhausted or lease expired',
		result = NULL, execution_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
		WHERE (status = 'running' AND lease_expires_at <= clock_timestamp())
		OR (status = 'pending' AND attempts >= max_attempts)`)
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
