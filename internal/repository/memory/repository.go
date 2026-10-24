package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"taskflow/internal/task"
)

type Repository struct {
	mu       sync.RWMutex
	tasks    map[string]task.Task
	capacity int
}

func New(capacity int) *Repository {
	return &Repository{tasks: make(map[string]task.Task), capacity: capacity}
}

func (r *Repository) Create(ctx context.Context, created task.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(r.tasks) >= r.capacity {
		return task.ErrCapacity
	}
	r.tasks[created.ID] = created
	return nil
}

func (r *Repository) Get(ctx context.Context, id string) (task.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return task.Task{}, err
	}
	found, ok := r.tasks[id]
	if !ok {
		return task.Task{}, task.ErrNotFound
	}
	return found, nil
}

func (r *Repository) List(ctx context.Context, options task.ListOptions) ([]task.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tasks := make([]task.Task, 0, len(r.tasks))
	for _, stored := range r.tasks {
		tasks = append(tasks, stored)
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].CreatedAt.Equal(tasks[j].CreatedAt) {
			return tasks[i].ID > tasks[j].ID
		}
		return tasks[i].CreatedAt.After(tasks[j].CreatedAt)
	})
	if options.Offset >= len(tasks) {
		return []task.Task{}, nil
	}
	end := min(options.Offset+options.Limit, len(tasks))
	return tasks[options.Offset:end], nil
}

func (r *Repository) Retry(ctx context.Context, id string) (task.Task, error) {
	return r.transition(ctx, id, true)
}

func (r *Repository) Cancel(ctx context.Context, id string) (task.Task, error) {
	return r.transition(ctx, id, false)
}

func (r *Repository) transition(ctx context.Context, id string, retry bool) (task.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return task.Task{}, err
	}
	current, ok := r.tasks[id]
	if !ok {
		return task.Task{}, task.ErrNotFound
	}
	if (retry && current.Status != task.StatusFailed) || (!retry && current.Status != task.StatusPending) {
		return task.Task{}, task.ErrConflict
	}
	current.UpdatedAt = time.Now().UTC().Truncate(time.Microsecond)
	current.NextRunAt = time.Time{}
	current.LastError = ""
	if retry {
		current.Status, current.Attempts = task.StatusPending, 0
		current.StartedAt, current.FinishedAt = time.Time{}, time.Time{}
		current.Result = task.SleepResult{}
		current.ExecutionID, current.LeaseExpiresAt = "", time.Time{}
	} else {
		current.Status, current.FinishedAt = task.StatusCancelled, current.UpdatedAt
	}
	r.tasks[id] = current
	return current, nil
}
