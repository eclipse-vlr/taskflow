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
)

type recoveryFunc func(context.Context) (int64, error)

func (f recoveryFunc) RecoverExpired(ctx context.Context) (int64, error) { return f(ctx) }

func TestRecoveryRetriesAndStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		repository := recoveryFunc(func(context.Context) (int64, error) {
			if calls.Add(1) == 1 {
				return 0, errors.New("temporary database failure")
			}
			return 1, nil
		})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- scheduler.RunRecovery(ctx, repository, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
		}()
		synctest.Wait()
		if calls.Load() != 0 {
			t.Fatal("recovery ran before its interval")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		if calls.Load() != 2 {
			t.Fatalf("recovery calls = %d", calls.Load())
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		if calls.Load() != 2 {
			t.Fatal("recovery continued after shutdown")
		}
	})
}
