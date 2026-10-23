package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"taskflow/internal/task"
)

type Source interface {
	Next(context.Context) (task.Task, error)
}

type Finisher interface {
	Finish(context.Context, string, int, task.Outcome) error
}

type Executor func(context.Context, task.Task) (task.SleepResult, error)

type Pool struct {
	count         int
	source        Source
	finisher      Finisher
	execute       Executor
	finishTimeout time.Duration
	logger        *slog.Logger
}

func New(count int, source Source, finisher Finisher, execute Executor, finishTimeout time.Duration, logger *slog.Logger) (*Pool, error) {
	if count < 1 || count > 100 || finishTimeout <= 0 {
		return nil, errors.New("invalid worker count or finish timeout")
	}
	return &Pool{count: count, source: source, finisher: finisher, execute: execute, finishTimeout: finishTimeout, logger: logger}, nil
}

// Run owns exactly count workers and joins every worker before returning.
// A persistence failure stops the pool instead of accepting more work.
func (p *Pool) Run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	errorsFound := make(chan error, p.count)
	var workers sync.WaitGroup
	for range p.count {
		workers.Go(func() {
			if err := p.runWorker(ctx); err != nil {
				errorsFound <- err
				cancel()
			}
		})
	}
	workers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		return err
	}
	return nil
}

func (p *Pool) runWorker(ctx context.Context) error {
	for ctx.Err() == nil {
		claimed, err := p.source.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("get next task: %w", err)
		}
		result, executionErr := p.execute(ctx, claimed)
		outcome := task.Outcome{Status: task.StatusSucceeded, Result: result}
		if executionErr != nil {
			outcome = task.Outcome{Status: task.StatusFailed, LastError: executionErr.Error()}
		}
		// State persistence outlives process cancellation, but has its own bound.
		finishCtx, cancel := context.WithTimeout(context.Background(), p.finishTimeout)
		err = p.finisher.Finish(finishCtx, claimed.ID, claimed.Attempts, outcome)
		cancel()
		if err != nil {
			return fmt.Errorf("persist execution of task %s: %w", claimed.ID, err)
		}
		p.logger.Info("task finished", "task_id", claimed.ID, "attempt", claimed.Attempts, "status", outcome.Status)
	}
	return nil
}
