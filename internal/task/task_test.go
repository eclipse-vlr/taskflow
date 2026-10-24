package task_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"taskflow/internal/repository/memory"
	"taskflow/internal/task"
)

func TestRetryDelay(t *testing.T) {
	for _, test := range []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second}, {1, time.Second}, {2, 2 * time.Second}, {5, 16 * time.Second},
		{6, 30 * time.Second}, {10, 30 * time.Second}, {1000000, 30 * time.Second},
	} {
		if got := task.RetryDelay(test.attempt); got != test.want {
			t.Errorf("delay(%d) = %s, want %s", test.attempt, got, test.want)
		}
	}
}

func TestCreateBoundaries(t *testing.T) {
	for _, tt := range []struct {
		duration, attempts int
	}{
		{1, 1}, {60000, 10},
	} {
		service := task.NewService(memory.New(1))
		created, err := service.Create(t.Context(), task.CreateInput{
			Type: "sleep", Payload: task.SleepPayload{DurationMS: tt.duration}, MaxAttempts: &tt.attempts,
		})
		if err != nil || created.Payload.DurationMS != tt.duration || created.MaxAttempts != tt.attempts {
			t.Fatalf("create = %+v, %v", created, err)
		}
	}
}

func TestCreateCancelled(t *testing.T) {
	service := task.NewService(memory.New(1))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	input := task.CreateInput{Type: "sleep", Payload: task.SleepPayload{DurationMS: 1}}
	if _, err := service.Create(ctx, input); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled create: %v", err)
	}
	if _, err := service.Create(t.Context(), input); err != nil {
		t.Fatalf("cancelled call consumed capacity: %v", err)
	}
}
