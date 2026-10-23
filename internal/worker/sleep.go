package worker

import (
	"context"
	"errors"
	"time"

	"taskflow/internal/task"
)

func Sleep(ctx context.Context, current task.Task) (task.SleepResult, error) {
	if err := ctx.Err(); err != nil {
		return task.SleepResult{}, err
	}
	if current.Type != "sleep" {
		return task.SleepResult{}, errors.New("unsupported task type")
	}
	if current.Payload.DurationMS < 1 || current.Payload.DurationMS > 60000 {
		return task.SleepResult{}, errors.New("invalid sleep duration")
	}
	timer := time.NewTimer(time.Duration(current.Payload.DurationMS) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return task.SleepResult{}, ctx.Err()
	case <-timer.C:
		return task.SleepResult{DurationMS: current.Payload.DurationMS}, nil
	}
}
