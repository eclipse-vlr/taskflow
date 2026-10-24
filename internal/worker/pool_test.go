package worker_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"taskflow/internal/task"
	"taskflow/internal/worker"
)

type sourceFunc func(context.Context) (task.Task, error)

func (f sourceFunc) Next(ctx context.Context) (task.Task, error) { return f(ctx) }

type finishFunc func(context.Context, string, int, task.Outcome) error

func (f finishFunc) Finish(ctx context.Context, id string, attempt int, outcome task.Outcome) error {
	return f(ctx, id, attempt, outcome)
}

func logger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPoolConcurrencyAndCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const count = 6
		jobs := make(chan task.Task, count)
		for i := range count {
			jobs <- task.Task{ID: fmt.Sprint(i), Attempts: 1}
		}
		source := sourceFunc(func(ctx context.Context) (task.Task, error) {
			select {
			case job := <-jobs:
				return job, nil
			case <-ctx.Done():
				return task.Task{}, ctx.Err()
			}
		})
		entered := make(chan string, count)
		release := make(chan struct{})
		finished := make(chan task.Outcome, count)
		var active, maximum atomic.Int32
		execute := func(ctx context.Context, current task.Task) (task.SleepResult, error) {
			n := active.Add(1)
			defer active.Add(-1)
			for old := maximum.Load(); n > old; old = maximum.Load() {
				if maximum.CompareAndSwap(old, n) {
					break
				}
			}
			entered <- current.ID
			select {
			case <-release:
				return task.SleepResult{DurationMS: 1}, nil
			case <-ctx.Done():
				return task.SleepResult{}, ctx.Err()
			}
		}
		finish := finishFunc(func(ctx context.Context, _ string, attempt int, outcome task.Outcome) error {
			if ctx.Err() != nil || attempt != 1 {
				t.Errorf("invalid completion context/attempt: %v, %d", ctx.Err(), attempt)
			}
			finished <- outcome
			return nil
		})
		pool, err := worker.New(2, source, finish, execute, time.Second, logger())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- pool.Run(ctx) }()
		synctest.Wait()
		if active.Load() != 2 || len(entered) != 2 || len(jobs) != count-2 {
			t.Fatalf("capacity exceeded: active=%d entered=%d pending=%d", active.Load(), len(entered), len(jobs))
		}
		for range count {
			release <- struct{}{}
			outcome := <-finished
			if outcome.Status != task.StatusSucceeded || outcome.Result.DurationMS != 1 {
				t.Fatalf("unexpected outcome: %+v", outcome)
			}
		}
		synctest.Wait()
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if maximum.Load() != 2 || active.Load() != 0 || len(jobs) != 0 {
			t.Fatalf("maximum=%d active=%d pending=%d", maximum.Load(), active.Load(), len(jobs))
		}
	})
}

func TestPoolPersistsFailureAndCancellation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		cancel bool
	}{
		{"handler failure", false}, {"shutdown", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls atomic.Int32
				source := sourceFunc(func(ctx context.Context) (task.Task, error) {
					if calls.Add(1) == 1 {
						return task.Task{ID: "one", Attempts: 1}, nil
					}
					<-ctx.Done()
					return task.Task{}, ctx.Err()
				})
				outcomes := make(chan task.Outcome, 1)
				finish := finishFunc(func(ctx context.Context, _ string, _ int, outcome task.Outcome) error {
					if ctx.Err() != nil {
						t.Error("completion inherited cancelled process context")
					}
					outcomes <- outcome
					return nil
				})
				execute := func(ctx context.Context, _ task.Task) (task.SleepResult, error) {
					if tt.cancel {
						<-ctx.Done()
						return task.SleepResult{}, ctx.Err()
					}
					return task.SleepResult{}, errors.New("handler failed")
				}
				pool, err := worker.New(1, source, finish, execute, time.Second, logger())
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- pool.Run(ctx) }()
				synctest.Wait()
				cancel()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				outcome := <-outcomes
				if outcome.Status != task.StatusFailed || outcome.LastError == "" || outcome.Result != (task.SleepResult{}) {
					t.Fatalf("failure not persisted: %+v", outcome)
				}
			})
		})
	}
}

func TestPoolStopsOnPersistenceError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := errors.New("database write failed")
		source := sourceFunc(func(context.Context) (task.Task, error) { return task.Task{ID: "one", Attempts: 1}, nil })
		finish := finishFunc(func(context.Context, string, int, task.Outcome) error { return failure })
		execute := func(context.Context, task.Task) (task.SleepResult, error) {
			return task.SleepResult{DurationMS: 1}, nil
		}
		pool, err := worker.New(2, source, finish, execute, time.Second, logger())
		if err != nil {
			t.Fatal(err)
		}
		if err := pool.Run(t.Context()); !errors.Is(err, failure) {
			t.Fatalf("persistence failure returned %v", err)
		}
	})
}
