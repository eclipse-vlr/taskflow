package handler_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"taskflow/internal/handler"
	"taskflow/internal/repository/memory"
	"taskflow/internal/task"
)

func TestTaskCommands(t *testing.T) {
	for _, test := range []struct {
		name, command string
		initial, want task.Status
		code          int
	}{
		{"cancel pending", "cancel", task.StatusPending, task.StatusCancelled, 200},
		{"cancel running", "cancel", task.StatusRunning, task.StatusRunning, 409},
		{"cancel finished", "cancel", task.StatusSucceeded, task.StatusSucceeded, 409},
		{"cancel twice", "cancel", task.StatusCancelled, task.StatusCancelled, 409},
		{"retry failed", "retry", task.StatusFailed, task.StatusPending, 202},
		{"retry pending", "retry", task.StatusPending, task.StatusPending, 409},
		{"retry running", "retry", task.StatusRunning, task.StatusRunning, 409},
		{"retry succeeded", "retry", task.StatusSucceeded, task.StatusSucceeded, 409},
		{"retry cancelled", "retry", task.StatusCancelled, task.StatusCancelled, 409},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := memory.New(10)
			now := time.Now().UTC().Truncate(time.Microsecond)
			initial := task.Task{ID: "one", Type: "sleep", Payload: task.SleepPayload{DurationMS: 1}, Status: test.initial,
				Attempts: 3, MaxAttempts: 3, LastError: "failed", StartedAt: now, FinishedAt: now, NextRunAt: now}
			if err := repository.Create(t.Context(), initial); err != nil {
				t.Fatal(err)
			}
			h := handler.New(task.NewService(repository), slog.New(slog.NewTextHandler(io.Discard, nil)))
			w := request(h, http.MethodPost, "/api/v1/tasks/one/"+test.command, "", "")
			if w.Code != test.code {
				t.Fatalf("status = %d, body = %s", w.Code, w.Body)
			}
			stored, err := repository.Get(t.Context(), initial.ID)
			if err != nil || stored.Status != test.want {
				t.Fatalf("stored = %+v, %v", stored, err)
			}
			if test.code == 409 && stored != initial {
				t.Fatal("conflict changed the task")
			}
			if test.code == 202 {
				if stored.Attempts != 0 || stored.LastError != "" || !stored.FinishedAt.IsZero() || !stored.NextRunAt.IsZero() || w.Header().Get("Location") != "/api/v1/tasks/one" {
					t.Fatalf("retry did not reset state: %+v", stored)
				}
			}
			if test.code < 300 {
				var updated task.Task
				if err := json.Unmarshal(w.Body.Bytes(), &updated); err != nil || updated != stored {
					t.Fatalf("response differs: %+v, %v", updated, err)
				}
			}
		})
	}
}

func TestTaskCommandInputAndErrors(t *testing.T) {
	for _, command := range []string{"retry", "cancel"} {
		h := newHandler(1)
		path := "/api/v1/tasks/missing/" + command
		for _, test := range []struct {
			path, body string
			code       int
		}{
			{path, "", 404}, {path, "{}", 400}, {path + "?extra=1", "", 400},
			{path, strings.Repeat("x", 16*1024+1), 413},
		} {
			w := request(h, http.MethodPost, test.path, test.body, "")
			if w.Code != test.code {
				t.Fatalf("%s: status = %d, body = %s", test.path, w.Code, w.Body)
			}
		}
		h = handler.New(failingService{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		w := request(h, http.MethodPost, path, "", "")
		if w.Code != 500 || w.Body.String() != "{\"error\":\"internal server error\"}\n" {
			t.Fatalf("internal error response: %d, %s", w.Code, w.Body)
		}
	}
}
