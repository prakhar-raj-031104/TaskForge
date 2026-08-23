// Package handler holds the HTTP handlers. Each one does exactly four things:
// parse the request, call the service, map the result to a response, map the
// error to a status code. Business rules live in internal/job.
package handler

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/anjani-kr-singh-ai/taskforge/internal/httpx"
	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/logging"
)

// HeaderIdempotencyKey lets a client supply the key as a header, which is the
// convention Stripe popularised and what most HTTP clients expect.
const HeaderIdempotencyKey = "Idempotency-Key"

// Jobs serves the job endpoints.
type Jobs struct {
	svc          *job.Service
	maxBodyBytes int64
}

// NewJobs builds the handler. Dependencies arrive through the constructor, so a
// test can hand it a service backed by an in-memory repository.
func NewJobs(svc *job.Service, maxBodyBytes int64) *Jobs {
	return &Jobs{svc: svc, maxBodyBytes: maxBodyBytes}
}

// Create handles POST /api/v1/jobs.
//
// Returns 201 for a new job and 200 when an idempotency key matched an existing
// one. Distinguishing the two is genuinely useful: a client retrying after a
// network timeout learns whether its original request landed.
func (h *Jobs) Create(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req createJobRequest
	if err := httpx.DecodeJSON(w, r, &req, h.maxBodyBytes); err != nil {
		var decErr *httpx.DecodeError
		if errors.As(err, &decErr) {
			httpx.WriteError(ctx, w, decErr.Status, codeForStatus(decErr.Status), decErr.Message)
			return
		}
		logging.FromContext(ctx).ErrorContext(ctx, "decoding job request", "error", err)
		httpx.WriteInternalError(ctx, w)
		return
	}

	// The body wins over the header when both are present, because the body is
	// the more explicit statement of intent. Documented in the OpenAPI spec so
	// clients are not left guessing.
	idempotencyKey := req.IdempotencyKey
	if idempotencyKey == nil {
		if v := strings.TrimSpace(r.Header.Get(HeaderIdempotencyKey)); v != "" {
			idempotencyKey = &v
		}
	}

	created, isNew, err := h.svc.Create(ctx, job.CreateParams{
		Type:           req.Type,
		Payload:        req.Payload,
		Priority:       req.Priority,
		MaxRetries:     req.MaxRetries,
		ScheduledAt:    req.ScheduledAt,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		writeServiceError(ctx, w, err)
		return
	}

	status := http.StatusOK
	if isNew {
		status = http.StatusCreated
		// Location is what makes 201 useful: the client learns where the new
		// resource lives without having to construct the URL itself.
		w.Header().Set("Location", "/api/v1/jobs/"+created.ID.String())
	}

	httpx.WriteJSON(w, status, toJobResponse(created))
}

// Get handles GET /api/v1/jobs/{id}.
func (h *Jobs) Get(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// PathValue is the Go 1.22+ stdlib router extracting {id} from the pattern.
	// Before that this needed a third-party router or manual string slicing.
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpx.WriteError(ctx, w, http.StatusBadRequest, httpx.CodeBadRequest,
			"job id must be a UUID")
		return
	}

	j, err := h.svc.Get(ctx, id)
	if err != nil {
		writeServiceError(ctx, w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, toJobResponse(j))
}

// List handles GET /api/v1/jobs.
func (h *Jobs) List(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	filter := job.ListFilter{
		Type:  strings.TrimSpace(q.Get("type")),
		Limit: job.DefaultListLimit,
	}

	// Repeated ?status= parameters, plus comma-separated values inside each,
	// so both ?status=pending&status=running and ?status=pending,running work.
	for _, raw := range q["status"] {
		for _, part := range strings.Split(raw, ",") {
			if part = strings.TrimSpace(part); part != "" {
				filter.Statuses = append(filter.Statuses, job.Status(part))
			}
		}
	}

	var invalid httpx.ErrorBody
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			invalid.Fields = append(invalid.Fields, httpx.FieldError{
				Field: "limit", Message: "must be an integer",
			})
		} else {
			filter.Limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			invalid.Fields = append(invalid.Fields, httpx.FieldError{
				Field: "offset", Message: "must be an integer",
			})
		} else {
			filter.Offset = n
		}
	}
	if len(invalid.Fields) > 0 {
		httpx.WriteValidationError(ctx, w, invalid.Fields)
		return
	}

	jobs, err := h.svc.List(ctx, filter)
	if err != nil {
		writeServiceError(ctx, w, err)
		return
	}

	// Build a non-nil slice so the response is [] and never null. A client
	// iterating the field should not have to nil-check it.
	items := make([]jobResponse, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, toJobResponse(j))
	}

	httpx.WriteJSON(w, http.StatusOK, listJobsResponse{
		Jobs:   items,
		Count:  len(items),
		Limit:  filter.Limit,
		Offset: filter.Offset,
	})
}

// writeServiceError maps a domain error onto an HTTP response.
//
// Centralising the mapping means every endpoint reports the same condition the
// same way, and there is exactly one place where a new error type has to be
// considered.
func writeServiceError(ctx context.Context, w http.ResponseWriter, err error) {
	log := logging.FromContext(ctx)

	var validationErr *job.ValidationError
	switch {
	case errors.As(err, &validationErr):
		fields := make([]httpx.FieldError, 0, len(validationErr.Fields))
		for _, f := range validationErr.Fields {
			fields = append(fields, httpx.FieldError{Field: f.Field, Message: f.Message})
		}
		httpx.WriteValidationError(ctx, w, fields)

	case errors.Is(err, job.ErrNotFound):
		httpx.WriteError(ctx, w, http.StatusNotFound, httpx.CodeNotFound, "job not found")

	case errors.Is(err, job.ErrInvalidTransition):
		// 409 Conflict, not 400: the request was well formed, but the resource
		// is in a state that does not permit it. Replaying a job that is
		// already running is the canonical example.
		var transition *job.InvalidTransitionError
		message := "the job is not in a state that allows this operation"
		if errors.As(err, &transition) {
			message = "the job is " + transition.From.String() + ", not dead_letter, so it cannot be replayed"
		}
		httpx.WriteError(ctx, w, http.StatusConflict, httpx.CodeConflict, message)

	case errors.Is(err, context.DeadlineExceeded):
		// The request outlived its deadline. 503 with Retry-After tells a
		// client this is transient and roughly when to come back, which is far
		// more actionable than a bare 500.
		log.WarnContext(ctx, "request deadline exceeded", "error", err)
		w.Header().Set("Retry-After", "1")
		httpx.WriteError(ctx, w, http.StatusServiceUnavailable, httpx.CodeTimeout,
			"the request took too long, please retry")

	case errors.Is(err, context.Canceled):
		// The client hung up. Nobody is listening, so writing a body is
		// pointless; 499 is nginx's non-standard code for exactly this and it
		// keeps the metric honest instead of inflating the 5xx rate.
		log.InfoContext(ctx, "client cancelled request")
		w.WriteHeader(499)

	default:
		// Log the real error with the request id; tell the client nothing.
		log.ErrorContext(ctx, "unhandled service error", "error", err)
		httpx.WriteInternalError(ctx, w)
	}
}

func codeForStatus(status int) string {
	switch status {
	case http.StatusRequestEntityTooLarge:
		return httpx.CodePayloadTooLarge
	case http.StatusUnsupportedMediaType:
		return httpx.CodeUnsupportedMedia
	default:
		return httpx.CodeBadRequest
	}
}
