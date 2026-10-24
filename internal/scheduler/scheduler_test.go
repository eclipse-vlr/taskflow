package scheduler_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"taskflow/internal/scheduler"
	"taskflow/internal/task"
)

type claimFunc func(context.Context) (task.Task, error)

func (f claimFunc) ClaimNext(ctx context.Context) (task.Task, error) { return f(ctx) }

func TestNextWaitsAndRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		repository := claimFunc(func(context.Context) (task.Task, error) {
			calls++
			switch calls {
			case 1:
				return task.Task{}, task.ErrNoReady
			case 2:
				return task.Task{}, errors.New("temporary database failure")
			default:
				return task.Task{ID: "ready"}, nil
			}
		})
		source, err := scheduler.New(repository, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		current, err := source.Next(t.Context())
		if err != nil || current.ID != "ready" || calls != 3 || time.Since(start) != 2*time.Second {
			t.Fatalf("task=%+v error=%v calls=%d elapsed=%v", current, err, calls, time.Since(start))
		}
	})
}

func TestNextCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		repository := claimFunc(func(context.Context) (task.Task, error) {
			calls.Add(1)
			return task.Task{}, task.ErrNoReady
		})
		source, err := scheduler.New(repository, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { _, err := source.Next(ctx); done <- err }()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) || calls.Load() != 1 {
			t.Fatalf("error=%v calls=%d", err, calls.Load())
		}
	})
}
