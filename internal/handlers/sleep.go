// Package handlers holds the concrete job handlers.
//
// Nothing here is imported by internal/worker. The worker depends only on the
// job.Handler interface, and main wires the two together. That is the whole
// point of the registry: a new job type is a new file in this package plus one
// line in cmd/worker, and the worker core is untouched.
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/logging"
)

// TypeSleep is the registered job type for Sleep.
const TypeSleep = "sleep"

// maxSleep caps how long a sleep job may request. Without it, a single job
// could occupy an execution slot indefinitely, and a handful could stall a
// whole worker.
const maxSleep = 5 * time.Minute

// sleepPayload is the expected job payload: {"seconds": 2.5}
type sleepPayload struct {
	Seconds float64 `json:"seconds"`
}

// Sleep does nothing for a configurable duration.
//
// It exists to make concurrency observable: submit twenty five-second sleeps to
// a worker with concurrency 5 and you can watch the pool saturate, the claim
// loop stop asking for work, and jobs complete in waves of five.
type Sleep struct{}

// NewSleep returns the handler.
func NewSleep() *Sleep { return &Sleep{} }

// Handle implements job.Handler.
func (h *Sleep) Handle(ctx context.Context, j *job.Job) error {
	var p sleepPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		// A payload that cannot be parsed will not parse on the fourth attempt
		// either, so this is permanent and dead-letters immediately.
		return job.Permanent(fmt.Errorf("invalid sleep payload: %w", err))
	}

	if p.Seconds < 0 {
		return job.Permanent(fmt.Errorf("seconds must not be negative, got %v", p.Seconds))
	}

	d := time.Duration(p.Seconds * float64(time.Second))
	if d > maxSleep {
		return job.Permanent(fmt.Errorf("seconds must be at most %s, got %s", maxSleep, d))
	}

	logging.FromContext(ctx).DebugContext(ctx, "sleeping", "duration", d)

	timer := time.NewTimer(d)
	defer timer.Stop()

	// The select is what makes this handler well-behaved. time.Sleep(d) would
	// ignore cancellation entirely, so a job timeout or a shutdown would have
	// to wait out the full duration. Every handler in this system must watch
	// ctx.Done() the same way.
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		// Returning the context error, unwrapped and NOT marked permanent, so
		// the job is retried later.
		return fmt.Errorf("sleep interrupted: %w", ctx.Err())
	}
}
