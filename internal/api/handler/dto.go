package handler

import (
	"encoding/json"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
)

// The types in this file are the API's wire contract, kept deliberately
// separate from job.Job.
//
// It is tempting to slap JSON tags on the domain struct and return it directly.
// Doing so welds your public API to your database schema: renaming an internal
// field becomes a breaking API change, and every new internal column leaks to
// clients automatically. lease_epoch below is the concrete example — it is
// load-bearing internally and meaningless externally, so it simply is not here.

// createJobRequest is the POST /api/v1/jobs body.
//
// Optional fields are pointers so "absent" and "explicitly zero" stay
// distinguishable after decoding.
type createJobRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       *int            `json:"priority"`
	MaxRetries     *int            `json:"max_retries"`
	ScheduledAt    *time.Time      `json:"scheduled_at"`
	IdempotencyKey *string         `json:"idempotency_key"`
}

// jobResponse is how a job appears on the wire.
type jobResponse struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
	Status  string          `json:"status"`

	Priority   int `json:"priority"`
	Attempts   int `json:"attempts"`
	MaxRetries int `json:"max_retries"`

	ScheduledAt *time.Time `json:"scheduled_at,omitempty"`
	AvailableAt time.Time  `json:"available_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	FailedAt    *time.Time `json:"failed_at,omitempty"`

	LeaseUntil *time.Time `json:"lease_until,omitempty"`
	WorkerID   *string    `json:"worker_id,omitempty"`

	IdempotencyKey *string `json:"idempotency_key,omitempty"`
	LastError      *string `json:"last_error,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// toJobResponse converts a domain job into its wire representation.
func toJobResponse(j *job.Job) jobResponse {
	return jobResponse{
		ID:      j.ID.String(),
		Type:    j.Type,
		Payload: j.Payload,
		Status:  j.Status.String(),

		Priority:   j.Priority,
		Attempts:   j.Attempts,
		MaxRetries: j.MaxRetries,

		ScheduledAt: utcPtr(j.ScheduledAt),
		AvailableAt: j.AvailableAt.UTC(),
		StartedAt:   utcPtr(j.StartedAt),
		CompletedAt: utcPtr(j.CompletedAt),
		FailedAt:    utcPtr(j.FailedAt),

		LeaseUntil: utcPtr(j.LeaseUntil),
		WorkerID:   j.WorkerID,

		IdempotencyKey: j.IdempotencyKey,
		LastError:      j.LastError,

		CreatedAt: j.CreatedAt.UTC(),
		UpdatedAt: j.UpdatedAt.UTC(),
	}
}

// utcPtr normalises an optional timestamp to UTC.
//
// A timestamptz read back through pgx arrives as a time.Time in the process's
// LOCAL zone. The instant is correct — 16:01+05:30 and 10:31Z are the same
// moment — but rendering it in the server's zone leaks where the server happens
// to be deployed and forces every client to handle arbitrary offsets. The API
// therefore always emits Z, and time.Time.UTC changes only the display zone,
// never the instant.
func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	utc := t.UTC()
	return &utc
}

// listJobsResponse wraps a page of results.
//
// A JSON array is never returned at the top level. An object leaves room to add
// pagination metadata without breaking every client, and it sidesteps the
// historical JSON-array-hijacking issue in browsers.
type listJobsResponse struct {
	Jobs   []jobResponse `json:"jobs"`
	Count  int           `json:"count"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

// healthResponse is returned by the liveness and readiness endpoints.
type healthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}
