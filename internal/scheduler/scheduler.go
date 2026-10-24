package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"taskflow/internal/task"
)

type Repository interface {
	ClaimNext(context.Context) (task.Task, error)
}

type Scheduler struct {
	repository Repository
	interval   time.Duration
	logger     *slog.Logger
}

func New(repository Repository, interval time.Duration, logger *slog.Logger) (*Scheduler, error) {
	if interval <= 0 {
		return nil, errors.New("scheduler interval must be positive")
	}
	return &Scheduler{repository: repository, interval: interval, logger: logger}, nil
}

// Next is called only by an idle worker. No task is claimed ahead of capacity.
func (s *Scheduler) Next(ctx context.Context) (task.Task, error) {
	for {
		if err := ctx.Err(); err != nil {
			return task.Task{}, err
		}
		claimed, err := s.repository.ClaimNext(ctx)
		if err == nil {
			return claimed, nil
		}
		if ctx.Err() != nil {
			return task.Task{}, ctx.Err()
		}
		if !errors.Is(err, task.ErrNoReady) {
			s.logger.ErrorContext(ctx, "claim task failed; retrying after poll interval", "error", err)
		}
		timer := time.NewTimer(s.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return task.Task{}, ctx.Err()
		case <-timer.C:
		}
	}
}
