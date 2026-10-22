package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"taskflow/internal/handler"
	"taskflow/internal/repository/memory"
	"taskflow/internal/task"
)

const validBody = `{"type":"sleep","payload":{"duration_ms":1000}}`

func newHandler(capacity int) http.Handler {
	return handler.New(task.NewService(memory.New(capacity)), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func request(h http.Handler, method, path, body, contentType string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestCreateAndGet(t *testing.T) {
	h := newHandler(10)
	w := request(h, http.MethodPost, "/api/v1/tasks", validBody, "application/json; charset=utf-8")
	if w.Code != http.StatusAccepted {
		t.Fatalf("create status = %d, body = %s", w.Code, w.Body)
	}
	var created task.Task
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if len(created.ID) != 36 || created.ID[14] != '4' || !strings.ContainsRune("89ab", rune(created.ID[19])) {
		t.Fatalf("expected UUID v4, got %q", created.ID)
	}
	if created.Type != "sleep" || created.Payload.DurationMS != 1000 || created.Status != task.StatusPending || created.Attempts != 0 || created.MaxAttempts != 3 {
		t.Fatalf("unexpected task: %+v", created)
	}
	if created.CreatedAt.IsZero() || !created.CreatedAt.Equal(created.UpdatedAt) {
		t.Fatalf("invalid timestamps: %+v", created)
	}
	location := "/api/v1/tasks/" + created.ID
	if w.Header().Get("Location") != location {
		t.Fatalf("Location = %q", w.Header().Get("Location"))
	}
	w = request(h, http.MethodGet, location, "", "")
	if w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("get status = %d, headers = %v", w.Code, w.Header())
	}
	var found task.Task
	if err := json.Unmarshal(w.Body.Bytes(), &found); err != nil {
		t.Fatal(err)
	}
	if found != created {
		t.Fatalf("stored task differs: got %+v, want %+v", found, created)
	}
}

func TestInvalidRequestsDoNotConsumeCapacity(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"empty", ""},
		{"malformed", `{"type":"sleep"`},
		{"null", `null`},
		{"array", `[]`},
		{"trailing object", validBody + `{}`},
		{"trailing garbage", validBody + `x`},
		{"unknown field", `{"type":"sleep","payload":{"duration_ms":1},"extra":true}`},
		{"unknown payload field", `{"type":"sleep","payload":{"duration_ms":1,"extra":1}}`},
		{"duplicate type", `{"type":"other","type":"sleep","payload":{"duration_ms":1}}`},
		{"case alias", `{"type":"other","Type":"sleep","payload":{"duration_ms":1}}`},
		{"duplicate duration", `{"type":"sleep","payload":{"duration_ms":0,"duration_ms":1}}`},
		{"unknown type", `{"type":"webhook","payload":{"duration_ms":1}}`},
		{"missing payload", `{"type":"sleep"}`},
		{"zero duration", `{"type":"sleep","payload":{"duration_ms":0}}`},
		{"negative duration", `{"type":"sleep","payload":{"duration_ms":-1}}`},
		{"long duration", `{"type":"sleep","payload":{"duration_ms":60001}}`},
		{"fractional duration", `{"type":"sleep","payload":{"duration_ms":1.5}}`},
		{"overflow", `{"type":"sleep","payload":{"duration_ms":9999999999999999999999}}`},
		{"zero attempts", `{"type":"sleep","payload":{"duration_ms":1},"max_attempts":0}`},
		{"too many attempts", `{"type":"sleep","payload":{"duration_ms":1},"max_attempts":11}`},
		{"invalid utf8", "{\"type\":\"sleep\",\"payload\":{\"duration_ms\":1},\"\xff\":1}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHandler(1)
			w := request(h, http.MethodPost, "/api/v1/tasks", tt.body, "application/json")
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", w.Code, w.Body)
			}
			var failure map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &failure); err != nil || failure["error"] == "" {
				t.Fatalf("expected JSON error, got %s", w.Body)
			}
			if w := request(h, http.MethodPost, "/api/v1/tasks", validBody, "application/json"); w.Code != http.StatusAccepted {
				t.Fatalf("invalid request consumed capacity: %d %s", w.Code, w.Body)
			}
		})
	}
}

func TestBodyLimitsAndMedia(t *testing.T) {
	tests := []struct {
		name, body, contentType, encoding string
		status                            int
	}{
		{"exact limit", validBody + strings.Repeat(" ", 16*1024-len(validBody)), "application/json", "", 202},
		{"over limit", validBody + strings.Repeat(" ", 16*1024-len(validBody)+1), "application/json", "", 413},
		{"missing media", validBody, "", "", 415},
		{"wrong media", validBody, "text/plain", "", 415},
		{"wrong charset", validBody, "application/json; charset=iso-8859-1", "", 415},
		{"compression", validBody, "application/json", "gzip", 415},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHandler(1)
			r := httptest.NewRequest(http.MethodPost, "/api/v1/tasks", strings.NewReader(tt.body))
			r.Header.Set("Content-Type", tt.contentType)
			r.Header.Set("Content-Encoding", tt.encoding)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tt.status {
				t.Fatalf("status = %d, want %d, body = %s", w.Code, tt.status, w.Body)
			}
			if tt.status != 202 {
				if w := request(h, http.MethodPost, "/api/v1/tasks", validBody, "application/json"); w.Code != 202 {
					t.Fatalf("rejected request consumed capacity: %d", w.Code)
				}
			}
		})
	}
}

func TestRoutesAndCapacity(t *testing.T) {
	h := newHandler(1)
	for _, tt := range []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/healthz", 200},
		{http.MethodGet, "/api/v1/tasks/missing", 404},
		{http.MethodGet, "/unknown", 404},
		{http.MethodDelete, "/api/v1/tasks/some-id", 405},
		{http.MethodPost, "/api/v1/tasks", 202},
		{http.MethodPost, "/api/v1/tasks", 503},
	} {
		w := request(h, tt.method, tt.path, validBody, "application/json")
		if w.Code != tt.status {
			t.Fatalf("%s %s: status = %d, want %d", tt.method, tt.path, w.Code, tt.status)
		}
	}
}

type failingService struct{}

func (failingService) Create(context.Context, task.CreateInput) (task.Task, error) {
	return task.Task{}, errors.New("password=secret")
}

func (failingService) Get(context.Context, string) (task.Task, error) {
	return task.Task{}, errors.New("password=secret")
}

func TestInternalErrorDoesNotLeak(t *testing.T) {
	h := handler.New(failingService{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		path := "/api/v1/tasks"
		if method == http.MethodGet {
			path += "/some-id"
		}
		w := request(h, method, path, validBody, "application/json")
		if w.Code != 500 || strings.Contains(w.Body.String(), "secret") {
			t.Fatalf("status = %d, body = %s", w.Code, w.Body)
		}
	}
}
