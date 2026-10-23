package task

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

var (
	ErrInvalid  = errors.New("invalid task")
	ErrNotFound = errors.New("task not found")
	ErrCapacity = errors.New("task capacity reached")
)

type SleepPayload struct {
	DurationMS int `json:"duration_ms"`
}

type CreateInput struct {
	Type        string
	Payload     SleepPayload
	MaxAttempts *int
}

type Task struct {
	ID          string       `json:"id"`
	Type        string       `json:"type"`
	Payload     SleepPayload `json:"payload"`
	Status      Status       `json:"status"`
	Attempts    int          `json:"attempts"`
	MaxAttempts int          `json:"max_attempts"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
}

type Repository interface {
	Create(context.Context, Task) error
	Get(context.Context, string) (Task, error)
	List(context.Context, ListOptions) ([]Task, error)
}

type ListOptions struct {
	Limit  int
	Offset int
}

type Service struct {
	repository Repository
}

func NewService(repository Repository) *Service {
	return &Service{repository: repository}
}

func (s *Service) Create(ctx context.Context, input CreateInput) (Task, error) {
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	if input.Type != "sleep" {
		return Task{}, fmt.Errorf("%w: type must be sleep", ErrInvalid)
	}
	if input.Payload.DurationMS < 1 || input.Payload.DurationMS > 60000 {
		return Task{}, fmt.Errorf("%w: duration_ms must be from 1 to 60000", ErrInvalid)
	}
	maxAttempts := 3
	if input.MaxAttempts != nil {
		maxAttempts = *input.MaxAttempts
	}
	if maxAttempts < 1 || maxAttempts > 10 {
		return Task{}, fmt.Errorf("%w: max_attempts must be from 1 to 10", ErrInvalid)
	}
	id, err := newID()
	if err != nil {
		return Task{}, fmt.Errorf("generate task ID: %w", err)
	}
	// PostgreSQL stores timestamps with microsecond precision.
	now := time.Now().UTC().Truncate(time.Microsecond)
	created := Task{
		ID: id, Type: input.Type, Payload: input.Payload,
		Status: StatusPending, MaxAttempts: maxAttempts,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := s.repository.Create(ctx, created); err != nil {
		return Task{}, fmt.Errorf("create task: %w", err)
	}
	return created, nil
}

func (s *Service) Get(ctx context.Context, id string) (Task, error) {
	found, err := s.repository.Get(ctx, id)
	if err != nil {
		return Task{}, fmt.Errorf("get task: %w", err)
	}
	return found, nil
}

func (s *Service) List(ctx context.Context, options ListOptions) ([]Task, error) {
	if options.Limit < 1 || options.Limit > 100 || options.Offset < 0 || options.Offset > 1000000 {
		return nil, fmt.Errorf("%w: limit must be from 1 to 100 and offset from 0 to 1000000", ErrInvalid)
	}
	tasks, err := s.repository.List(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	return tasks, nil
}

func newID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[:4], value[4:6], value[6:8], value[8:10], value[10:]), nil
}
