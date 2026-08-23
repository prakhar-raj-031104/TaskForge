package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
)

// TypeFlaky is the registered job type for Flaky.
const TypeFlaky = "flaky"

// flakyPayload is the expected job payload:
//
//	{"fail_rate": 0.7, "permanent": false, "panic": false}
type flakyPayload struct {
	// FailRate is the probability in [0,1] that an attempt fails.
	FailRate float64 `json:"fail_rate"`
	// Permanent makes the failure non-retryable, so the job dead-letters on the
	// first attempt.
	Permanent bool `json:"permanent"`
	// Panic makes the handler panic instead of returning an error, exercising
	// the worker's panic recovery.
	Panic bool `json:"panic"`
}

// Flaky fails on demand.
//
// This is a test and demonstration handler, not a production one. It is what
// makes retries, exponential backoff, the dead-letter queue and panic recovery
// observable end to end without having to break a real dependency:
//
//	{"type":"flaky","payload":{"fail_rate":1.0},"max_retries":3}
//
// submits a job that always fails, retries three times with growing backoff,
// and then lands in the dead-letter queue.
type Flaky struct{}

// NewFlaky returns the handler.
func NewFlaky() *Flaky { return &Flaky{} }

// Handle implements job.Handler.
func (h *Flaky) Handle(_ context.Context, j *job.Job) error {
	var p flakyPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return job.Permanent(fmt.Errorf("invalid flaky payload: %w", err))
	}

	if p.FailRate < 0 || p.FailRate > 1 {
		return job.Permanent(fmt.Errorf("fail_rate must be between 0 and 1, got %v", p.FailRate))
	}

	if rand.Float64() >= p.FailRate {
		return nil
	}

	if p.Panic {
		panic(fmt.Sprintf("flaky job %s asked to panic on attempt %d", j.ID, j.Attempts))
	}

	err := fmt.Errorf("flaky job failed on attempt %d of %d: %w",
		j.Attempts, j.MaxRetries+1, errors.New("simulated failure"))

	if p.Permanent {
		return job.Permanent(err)
	}
	return err
}
