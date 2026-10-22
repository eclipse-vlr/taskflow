package memory_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"taskflow/internal/repository/memory"
	"taskflow/internal/task"
)

func TestConcurrentCapacityAndReads(t *testing.T) {
	const capacity = 10
	repository := memory.New(capacity)
	var accepted atomic.Int32
	var workers sync.WaitGroup
	start := make(chan struct{})
	for i := range 100 {
		workers.Go(func() {
			<-start
			created := task.Task{ID: fmt.Sprint(i), Status: task.StatusPending}
			err := repository.Create(t.Context(), created)
			if errors.Is(err, task.ErrCapacity) {
				return
			}
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			accepted.Add(1)
			found, err := repository.Get(t.Context(), created.ID)
			if err != nil || found != created {
				t.Errorf("get = %+v, %v; want %+v", found, err, created)
			}
		})
	}
	close(start)
	workers.Wait()
	if accepted.Load() != capacity {
		t.Fatalf("accepted = %d, want %d", accepted.Load(), capacity)
	}
}

func TestCancellationAndCopies(t *testing.T) {
	repository := memory.New(1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	created := task.Task{ID: "task-1", Payload: task.SleepPayload{DurationMS: 10}}
	if err := repository.Create(ctx, created); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled create: %v", err)
	}
	if err := repository.Create(t.Context(), created); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Get(ctx, created.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled get: %v", err)
	}
	created.Payload.DurationMS = 20
	found, err := repository.Get(t.Context(), created.ID)
	if err != nil || found.Payload.DurationMS != 10 {
		t.Fatalf("input mutation changed task: %+v, %v", found, err)
	}
	found.Payload.DurationMS = 30
	found, err = repository.Get(t.Context(), created.ID)
	if err != nil || found.Payload.DurationMS != 10 {
		t.Fatalf("output mutation changed task: %+v, %v", found, err)
	}
	if _, err := repository.Get(t.Context(), "missing"); !errors.Is(err, task.ErrNotFound) {
		t.Fatalf("missing task: %v", err)
	}
}
