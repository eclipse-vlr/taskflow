package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	"taskflow/internal/task"
)

const maxBodyBytes = 16 * 1024

type TaskService interface {
	Create(context.Context, task.CreateInput) (task.Task, error)
	Get(context.Context, string) (task.Task, error)
	List(context.Context, task.ListOptions) ([]task.Task, error)
	Retry(context.Context, string) (task.Task, error)
	Cancel(context.Context, string) (task.Task, error)
}

type Handler struct {
	service TaskService
	logger  *slog.Logger
}

func New(service TaskService, logger *slog.Logger) http.Handler {
	h := &Handler{service: service, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /api/v1/tasks", h.create)
	mux.HandleFunc("GET /api/v1/tasks", h.list)
	mux.HandleFunc("GET /api/v1/tasks/{id}", h.get)
	mux.HandleFunc("POST /api/v1/tasks/{id}/retry", h.retry)
	mux.HandleFunc("POST /api/v1/tasks/{id}/cancel", h.cancel)
	return mux
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" || (params["charset"] != "" && !strings.EqualFold(params["charset"], "utf-8")) {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json with UTF-8 encoding")
		return
	}
	if encoding := r.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Encoding is not supported")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body exceeds 16 KiB")
			return
		}
		writeError(w, http.StatusBadRequest, "cannot read request body")
		return
	}
	var input struct {
		Type        string            `json:"type"`
		Payload     task.SleepPayload `json:"payload"`
		MaxAttempts *int              `json:"max_attempts"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || !utf8.Valid(body) || !uniqueKeys(body) {
		writeError(w, http.StatusBadRequest, "invalid JSON: use known, unique fields and one object")
		return
	}
	created, err := h.service.Create(r.Context(), task.CreateInput{
		Type: input.Type, Payload: input.Payload, MaxAttempts: input.MaxAttempts,
	})
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/v1/tasks/"+created.ID)
	writeJSON(w, http.StatusAccepted, created)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	found, err := h.service.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, found)
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	query, err := urlQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid pagination parameters")
		return
	}
	tasks, err := h.service.List(r.Context(), query)
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks, "limit": query.Limit, "offset": query.Offset})
}

func (h *Handler) retry(w http.ResponseWriter, r *http.Request) {
	h.transition(w, r, true)
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	h.transition(w, r, false)
}

func (h *Handler) transition(w http.ResponseWriter, r *http.Request, retry bool) {
	// These commands accept neither parameters nor a JSON body.
	if r.URL.RawQuery != "" {
		writeError(w, http.StatusBadRequest, "this command takes no query parameters")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body exceeds 16 KiB")
		} else {
			writeError(w, http.StatusBadRequest, "cannot read request body")
		}
		return
	}
	if len(body) != 0 {
		writeError(w, http.StatusBadRequest, "this command takes no request body")
		return
	}
	var updated task.Task
	var status int
	if retry {
		updated, err = h.service.Retry(r.Context(), r.PathValue("id"))
		status = http.StatusAccepted
	} else {
		updated, err = h.service.Cancel(r.Context(), r.PathValue("id"))
		status = http.StatusOK
	}
	if err != nil {
		h.serviceError(w, r, err)
		return
	}
	if retry {
		w.Header().Set("Location", "/api/v1/tasks/"+updated.ID)
	}
	writeJSON(w, status, updated)
}

func urlQuery(r *http.Request) (task.ListOptions, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return task.ListOptions{}, err
	}
	options := task.ListOptions{Limit: 20}
	for name, values := range values {
		if len(values) != 1 || (name != "limit" && name != "offset") {
			return task.ListOptions{}, task.ErrInvalid
		}
		value, err := strconv.Atoi(values[0])
		if err != nil {
			return task.ListOptions{}, err
		}
		if name == "limit" {
			options.Limit = value
		} else {
			options.Offset = value
		}
	}
	return options, nil
}

func (h *Handler) serviceError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, task.ErrInvalid):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, task.ErrNotFound):
		writeError(w, http.StatusNotFound, "task not found")
	case errors.Is(err, task.ErrConflict):
		writeError(w, http.StatusConflict, "task state does not allow this operation")
	case errors.Is(err, task.ErrCapacity):
		writeError(w, http.StatusServiceUnavailable, "task capacity reached")
	case errors.Is(err, context.Canceled):
		writeError(w, http.StatusRequestTimeout, "request cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, "operation timed out")
	default:
		h.logger.ErrorContext(r.Context(), "task request failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	// All response types here contain only JSON-encodable values. A write failure
	// means the connection is gone; a second response cannot repair it.
	_ = json.NewEncoder(w).Encode(value)
}

// uniqueKeys checks every object and rejects extra JSON values. EqualFold also
// rejects key aliases that encoding/json matches without regard to case.
func uniqueKeys(body []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(body))
	if !readValue(decoder) {
		return false
	}
	_, err := decoder.Token()
	return errors.Is(err, io.EOF)
}

func readValue(decoder *json.Decoder) bool {
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return true
	}
	switch delim {
	case '{':
		var seen []string
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return false
			}
			name, ok := key.(string)
			if !ok {
				return false
			}
			for _, previous := range seen {
				if strings.EqualFold(previous, name) {
					return false
				}
			}
			seen = append(seen, name)
			if !readValue(decoder) {
				return false
			}
		}
	case '[':
		for decoder.More() {
			if !readValue(decoder) {
				return false
			}
		}
	default:
		return false
	}
	_, err = decoder.Token()
	return err == nil
}
