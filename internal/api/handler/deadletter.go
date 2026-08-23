package handler

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/anjani-kr-singh-ai/taskforge/internal/httpx"
	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
)

// DeadLetter serves the dead-letter inspection and replay endpoints.
//
// These are operator endpoints. When something breaks at 3am, the first two
// questions are always "what failed" and "can I replay it once I have fixed the
// cause"; this is the answer to both.
type DeadLetter struct {
	svc *job.Service
}

// NewDeadLetter builds the handler.
func NewDeadLetter(svc *job.Service) *DeadLetter {
	return &DeadLetter{svc: svc}
}

// List handles GET /api/v1/dead-letter-jobs.
func (h *DeadLetter) List(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	filter := job.ListFilter{
		Type:  strings.TrimSpace(q.Get("type")),
		Limit: job.DefaultListLimit,
	}

	var fields []httpx.FieldError
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			fields = append(fields, httpx.FieldError{Field: "limit", Message: "must be an integer"})
		} else {
			filter.Limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			fields = append(fields, httpx.FieldError{Field: "offset", Message: "must be an integer"})
		} else {
			filter.Offset = n
		}
	}
	if len(fields) > 0 {
		httpx.WriteValidationError(ctx, w, fields)
		return
	}

	jobs, err := h.svc.ListDeadLetter(ctx, filter)
	if err != nil {
		writeServiceError(ctx, w, err)
		return
	}

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

// Retry handles POST /api/v1/dead-letter-jobs/{id}/retry.
//
// POST rather than PUT: this is not idempotent in the HTTP sense. Replaying an
// already-replayed job is refused with 409 precisely because the second call
// would otherwise reset a job that is currently running.
func (h *DeadLetter) Retry(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpx.WriteError(ctx, w, http.StatusBadRequest, httpx.CodeBadRequest,
			"job id must be a UUID")
		return
	}

	j, err := h.svc.RetryDeadLetter(ctx, id)
	if err != nil {
		writeServiceError(ctx, w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, toJobResponse(j))
}
