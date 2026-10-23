package worker_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"taskflow/internal/task"
	"taskflow/internal/worker"
)

func TestSleepDuration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		result, err := worker.Sleep(t.Context(), task.Task{Type: "sleep", Payload: task.SleepPayload{DurationMS: 1000}})
		if err != nil || result.DurationMS != 1000 || time.Since(start) != time.Second {
			t.Fatalf("result = %+v, error = %v, elapsed = %v", result, err, time.Since(start))
		}
	})
}

func TestSleepCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := worker.Sleep(ctx, task.Task{Type: "sleep", Payload: task.SleepPayload{DurationMS: 60000}})
			done <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled sleep returned %v", err)
		}
	})
}

func TestSleepRejectsInvalidTask(t *testing.T) {
	for _, current := range []task.Task{
		{Type: "webhook", Payload: task.SleepPayload{DurationMS: 1}},
		{Type: "sleep", Payload: task.SleepPayload{DurationMS: 0}},
		{Type: "sleep", Payload: task.SleepPayload{DurationMS: 60001}},
	} {
		if _, err := worker.Sleep(t.Context(), current); err == nil {
			t.Fatalf("invalid task accepted: %+v", current)
		}
	}
}
