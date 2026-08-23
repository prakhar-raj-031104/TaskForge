// Package job holds the TaskForge domain: what a job is, which state
// transitions are legal, how a creation request is validated, and the
// Repository contract the rest of the system depends on.
//
// Domain, service and repository interface live in ONE package rather than
// three. Go's unit of encapsulation is the package; splitting a cohesive
// domain across domain/, service/ and repository/ folders forces you to export
// everything and invites import cycles the moment the service needs a domain
// type and the domain needs a service error. The pgx implementation lives in
// the job/postgres sub-package because that is a genuine dependency boundary:
// it is the only code that imports pgx.
package job

import (
	"encoding/json"
	"slices"
	"time"

	"github.com/google/uuid"
)

// Status is the job state machine's alphabet.
type Status string

const (
	// StatusPending means claimable once AvailableAt has passed. It covers both
	// "never attempted" and "waiting out a retry backoff"; distinguish the two
	// with Attempts > 0.
	StatusPending Status = "pending"

	// StatusRunning means leased by a worker until LeaseUntil.
	StatusRunning Status = "running"

	// StatusCompleted means the handler returned nil. Terminal.
	StatusCompleted Status = "completed"

	// StatusDeadLetter means retries were exhausted, or the handler returned a
	// permanent error. Terminal until an operator replays it.
	StatusDeadLetter Status = "dead_letter"

	// StatusCancelled means an operator cancelled the job before it finished.
	StatusCancelled Status = "cancelled"
)

func (s Status) String() string { return string(s) }

var allStatuses = []Status{
	StatusPending, StatusRunning, StatusCompleted, StatusDeadLetter, StatusCancelled,
}

// Statuses returns every valid status. It returns a copy so a caller cannot
// mutate the package's own slice — returning the package-level slice directly
// would hand out a writable window into shared state.
func Statuses() []Status { return slices.Clone(allStatuses) }

// Valid reports whether s is a status this system understands.
func (s Status) Valid() bool { return slices.Contains(allStatuses, s) }

// IsTerminal reports whether a job in this status will never run again without
// operator action.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusCompleted, StatusDeadLetter, StatusCancelled:
		return true
	default:
		return false
	}
}

// transitions is the state machine, written out explicitly.
//
// An explicit transition table matters because the alternative — letting any
// code UPDATE status to anything — makes illegal states reachable, and illegal
// states in a queue are how jobs get executed twice or silently vanish. Every
// write path in this system checks against this table, and the database
// enforces the same invariants with CHECK constraints as a second line.
var transitions = map[Status][]Status{
	// A pending job is picked up by a worker, or cancelled before it runs.
	StatusPending: {StatusRunning, StatusCancelled},

	// A running job succeeds, goes back to pending for a retry, exhausts its
	// budget into the dead-letter queue, or is cancelled mid-flight.
	StatusRunning: {StatusCompleted, StatusPending, StatusDeadLetter, StatusCancelled},

	// Terminal.
	StatusCompleted: {},
	StatusCancelled: {},

	// Dead-lettered jobs are replayable by an operator, which is the entire
	// point of having a dead-letter queue rather than deleting the job.
	StatusDeadLetter: {StatusPending},
}

// CanTransitionTo reports whether s -> next is a legal move.
func (s Status) CanTransitionTo(next Status) bool {
	return slices.Contains(transitions[s], next)
}

// Domain limits. These are enforced here AND as CHECK constraints in migration
// 000001. Duplication is deliberate: the application gives a good error
// message, the database guarantees the invariant even for a hand-written UPDATE
// during an incident.
const (
	MinPriority = 1
	MaxPriority = 100
	// DefaultPriority sits in the middle so callers have room to go both ways.
	DefaultPriority = 50

	DefaultMaxRetries = 3
	MaxMaxRetries     = 100

	MaxTypeLength           = 100
	MaxIdempotencyKeyLength = 255

	// MaxScheduleAhead guards against a typo'd year (3026 instead of 2026)
	// silently parking a job for a millennium.
	MaxScheduleAhead = 365 * 24 * time.Hour

	// MaxLastErrorLength bounds what we persist from a handler error. Handler
	// errors can embed an entire HTTP response body; unbounded, that bloats the
	// hottest table in the system.
	MaxLastErrorLength = 2000
)

// Job is one unit of work, mirroring a row of the jobs table.
//
// Nullable columns are pointers so that "not set" and "set to the zero value"
// stay distinguishable — a *time.Time that is nil genuinely means NULL, whereas
// a zero time.Time would be indistinguishable from year 1.
type Job struct {
	ID      uuid.UUID
	Type    string
	Payload json.RawMessage
	Status  Status

	Priority   int
	Attempts   int
	MaxRetries int

	// ScheduledAt is user intent: what the client asked for. Immutable.
	ScheduledAt *time.Time
	// AvailableAt is system eligibility, rewritten by every retry backoff.
	// The claim query reads this and never ScheduledAt.
	AvailableAt time.Time

	StartedAt   *time.Time
	CompletedAt *time.Time
	FailedAt    *time.Time

	LeaseUntil *time.Time
	LeaseEpoch int64
	WorkerID   *string

	IdempotencyKey *string
	LastError      *string

	CreatedAt time.Time
	UpdatedAt time.Time
}

// RetriesRemaining reports how many attempts the job still has.
func (j *Job) RetriesRemaining() int {
	remaining := j.MaxRetries - j.Attempts
	if remaining < 0 {
		return 0
	}
	return remaining
}

// LeaseExpired reports whether the job's lease has lapsed as of now, meaning
// another worker may reclaim it.
func (j *Job) LeaseExpired(now time.Time) bool {
	return j.LeaseUntil != nil && now.After(*j.LeaseUntil)
}

// TruncateError shortens a handler error to MaxLastErrorLength so it is safe
// to persist.
func TruncateError(msg string) string {
	if len(msg) <= MaxLastErrorLength {
		return msg
	}
	return msg[:MaxLastErrorLength-3] + "..."
}
