package job

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// DefaultReclaimBatch bounds one reaper pass.
//
// Reaping in bounded batches rather than one enormous UPDATE keeps the
// transaction short, so it never blocks the claim path or holds a connection
// for long. After a mass outage the reaper simply runs several passes.
const DefaultReclaimBatch = 100

// Queue is the worker-side persistence contract.
//
// It is a SEPARATE interface from Repository even though one PostgreSQL type
// implements both. The API never claims or completes jobs; the worker never
// lists or creates them. Splitting the interfaces by consumer means each side
// depends only on what it uses, a fake in an API test does not have to stub out
// lease handling, and the compiler tells you immediately if a change to the
// worker path leaks into the request path.
//
// This is interface segregation applied for a practical reason rather than as
// ceremony.
type Queue interface {
	// Claim atomically reserves up to limit jobs for a worker.
	//
	// The claim must be safe against every other worker in the fleet running
	// the same query at the same instant: no job may be handed to two workers.
	Claim(ctx context.Context, p ClaimParams) ([]*Job, error)

	// Complete marks a leased job as finished.
	//
	// It must verify the lease. If the worker's lease expired and the job was
	// reclaimed by somebody else, Complete must return ErrLeaseLost and change
	// nothing.
	Complete(ctx context.Context, l Lease) error

	// Fail records a failed attempt and decides the job's next state: back to
	// pending after a backoff delay, or into the dead-letter queue once the
	// retry budget is spent. Like Complete, it must verify the lease.
	Fail(ctx context.Context, l Lease, f FailParams) (Status, error)

	// RenewLease extends a lease that is still held.
	//
	// A worker calls this periodically while a job runs, which is what lets the
	// lease be short (so a crash is detected quickly) without a slow-but-healthy
	// job being reclaimed underneath itself. It must verify the fencing epoch,
	// so a worker that already lost its lease cannot take it back.
	RenewLease(ctx context.Context, l Lease, d time.Duration) error

	// ReclaimExpired returns jobs whose lease has lapsed to the pending state,
	// or dead-letters them if their retry budget is spent.
	//
	// This is the crash recovery mechanism. A worker that is killed, panics at
	// the process level, loses its machine or is partitioned from the network
	// leaves its jobs marked running forever; nothing else in the system would
	// ever notice.
	ReclaimExpired(ctx context.Context, limit int) (ReclaimResult, error)
}

// ReclaimResult summarises one reaper pass.
type ReclaimResult struct {
	// Requeued is the number of jobs returned to pending.
	Requeued int
	// DeadLettered is the number that had no retries left.
	DeadLettered int
}

// Total returns how many jobs the pass touched.
func (r ReclaimResult) Total() int { return r.Requeued + r.DeadLettered }

// ClaimParams describes a claim request.
type ClaimParams struct {
	// WorkerID identifies the claiming process, recorded on each row.
	WorkerID string
	// Limit is the maximum number of jobs to claim, normally the number of
	// free execution slots.
	Limit int
	// LeaseDuration is how long the worker owns the claimed jobs.
	//
	// Sizing this is a genuine tradeoff. Too short and a slow-but-healthy job
	// gets reclaimed and executed twice. Too long and a crashed worker's jobs
	// sit invisible for that whole period. The lease is renewed while a job
	// runs, so it should be comfortably longer than the renewal interval, not
	// longer than the slowest job.
	LeaseDuration time.Duration

	// Aging optionally lets long-waiting jobs overtake newer high-priority
	// work. See AgingPolicy.
	Aging AgingPolicy
}

// AgingPolicy prevents starvation under strict priority ordering.
//
// # The problem
//
// `ORDER BY priority DESC` is exactly what you want until the moment
// high-priority work arrives faster than the fleet drains it. From then on
// there is always a priority-90 job available, so the priority-10 job submitted
// this morning is never the best candidate. It is not slow; it is never
// selected at all. That is starvation, and it is the default behaviour of every
// naive priority queue.
//
// # The fix
//
// Age the priority: a job's EFFECTIVE priority rises the longer it waits, so a
// low-priority job eventually outranks fresh high-priority work no matter how
// much of it arrives.
//
//	effective = priority + min(minutes_waited * BoostPerMinute, MaxBoost)
//
// # Why it is off by default
//
// The strict ordering (priority DESC, available_at ASC, id ASC) matches
// jobs_claim_idx exactly, so PostgreSQL walks the index in order and never
// sorts. Aging orders by a computed expression, which no index can satisfy, so
// the planner must read every claimable row and sort it.
//
// That is affordable precisely because the partial index keeps the pending set
// small - a healthy queue has hundreds of pending rows, not millions - but it
// IS a real cost, and it should be a deliberate choice rather than a default.
// Enable it when you actually run mixed priorities; leave it off when you do
// not. Both plans are measured in the README.
type AgingPolicy struct {
	// Enabled turns on the aged ordering.
	Enabled bool
	// BoostPerMinute is how much effective priority one minute of waiting adds.
	BoostPerMinute float64
	// MaxBoost caps the bonus, so an ancient priority-1 job cannot permanently
	// outrank genuinely urgent work.
	MaxBoost float64
}

// Lease is a worker's proof that it currently owns a job.
//
// Epoch is the fencing token. Every claim increments the row's lease_epoch, so
// a worker that was frozen past its lease expiry holds a stale epoch, and every
// write it attempts matches zero rows instead of overwriting the new owner's
// work. See docs/design.md section 6.
type Lease struct {
	JobID    uuid.UUID
	WorkerID string
	Epoch    int64
}

// FailParams describes a failed attempt.
type FailParams struct {
	// Cause is the error text stored in last_error. Truncated by the caller.
	Cause string
	// RetryDelay is how long to wait before the job becomes claimable again.
	// Ignored when the job is dead-lettered.
	RetryDelay time.Duration
	// Permanent forces the dead-letter queue regardless of remaining retries.
	Permanent bool
}
