package memory

import (
	"context"
	"sync"

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
