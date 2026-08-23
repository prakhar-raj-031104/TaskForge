package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
)

// The same *JobRepository implements both job.Repository (API side) and
// job.Queue (worker side). One type, two narrow contracts, each depended on by
// exactly the code that needs it.
var _ job.Queue = (*JobRepository)(nil)

// claimQuery is the heart of the entire system.
//
//	WITH claimed AS (
//	    SELECT id FROM jobs
//	     WHERE status = 'pending' AND available_at <= now()
//	     ORDER BY priority DESC, available_at ASC, id ASC
//	     FOR UPDATE SKIP LOCKED
//	     LIMIT $1
//	)
//	UPDATE jobs ... FROM claimed ...
//
// # Why FOR UPDATE SKIP LOCKED
//
// Twenty workers poll this query simultaneously. Consider the alternatives:
//
//   - Plain SELECT then UPDATE. Every worker reads the same top 10 rows,
//     every worker updates them, and the same job runs twenty times. Broken.
//
//   - SELECT ... FOR UPDATE (no SKIP LOCKED). Worker 1 locks the top 10 rows.
//     Workers 2 through 20 BLOCK on those exact rows, waiting for worker 1 to
//     commit. They then wake up, discover the rows are no longer pending, and
//     start over. Correct, but the fleet is serialised: throughput is that of
//     one worker no matter how many you run, and every added worker makes the
//     lock convoy worse.
//
//   - SERIALIZABLE isolation. Correct, but concurrent claims conflict and
//     Postgres resolves them by aborting transactions, so the workers spend
//     their time retrying serialisation failures.
//
//   - Advisory locks. Workable, but you are hand-rolling what SKIP LOCKED
//     already does, and the lock is not tied to the row's transaction.
//
// SKIP LOCKED changes "wait for that row" into "pretend that row is not there".
// Worker 1 takes rows 1-10, worker 2 immediately takes 11-20, worker 3 takes
// 21-30. No blocking, no conflict, no duplicate delivery, and throughput scales
// with the number of workers. It is the one feature that makes a relational
// database a genuinely good queue.
//
// # Why the CTE
//
// The lock must be taken by the SELECT, but the rows must be marked running in
// the same statement so no window exists in which a row is locked-but-still-
// pending. A single statement is implicitly its own transaction, so the lock is
// acquired and released around one round trip. Crucially the transaction does
// NOT stay open while the job executes — that would pin a connection for the
// job's whole duration and block autovacuum. The row lock protects the CLAIM;
// the lease protects the EXECUTION.
//
// # Why attempts increments here and not on failure
//
// A "poison" job that crashes its worker process never reaches any failure
// path. If attempts only grew on a recorded failure, such a job would be
// reclaimed forever, taking down every worker that touched it. Counting the
// attempt at claim time means even a job that kills the process consumes its
// retry budget and eventually dead-letters.
const claimQuery = `
	WITH claimed AS (
		SELECT id
		  FROM jobs
		 WHERE status = 'pending'
		   AND available_at <= now()
		 ORDER BY priority DESC, available_at ASC, id ASC
		 FOR UPDATE SKIP LOCKED
		 LIMIT $1
	)
	UPDATE jobs AS j
	   SET status      = 'running',
	       worker_id   = $2,
	       lease_until = now() + make_interval(secs => $3),
	       lease_epoch = j.lease_epoch + 1,
	       attempts    = j.attempts + 1,
	       started_at  = now()
	  FROM claimed c
	 WHERE j.id = c.id
	RETURNING ` + jobColumnsQualified

// claimAgingQuery is claimQuery with starvation control.
//
// The only difference is the ORDER BY, which ranks by an EFFECTIVE priority
// that grows with waiting time instead of the stored one:
//
//	priority + least(minutes_waited * $4, $5)
//
// The cost is stated plainly: this expression cannot be served by
// jobs_claim_idx, so the planner switches from an ordered index walk to reading
// every claimable row and sorting. That is affordable only because the partial
// index keeps the pending set small. Compare the two plans with
// scripts/explain_claim.sql before enabling it on a large queue.
const claimAgingQuery = `
	WITH claimed AS (
		SELECT id
		  FROM jobs
		 WHERE status = 'pending'
		   AND available_at <= now()
		 ORDER BY (priority + least(
		               extract(epoch FROM (now() - available_at)) / 60.0 * $4,
		               $5)) DESC,
		          available_at ASC, id ASC
		 FOR UPDATE SKIP LOCKED
		 LIMIT $1
	)
	UPDATE jobs AS j
	   SET status      = 'running',
	       worker_id   = $2,
	       lease_until = now() + make_interval(secs => $3),
	       lease_epoch = j.lease_epoch + 1,
	       attempts    = j.attempts + 1,
	       started_at  = now()
	  FROM claimed c
	 WHERE j.id = c.id
	RETURNING ` + jobColumnsQualified

// jobColumnsQualified is jobColumns with a j. prefix, needed because the UPDATE
// above joins two relations and bare column names would be ambiguous.
const jobColumnsQualified = `
	j.id, j.type, j.payload, j.status, j.priority, j.attempts, j.max_retries,
	j.scheduled_at, j.available_at, j.started_at, j.completed_at, j.failed_at,
	j.lease_until, j.lease_epoch, j.worker_id, j.idempotency_key, j.last_error,
	j.created_at, j.updated_at`

// Claim reserves up to p.Limit jobs for the worker.
func (r *JobRepository) Claim(ctx context.Context, p job.ClaimParams) ([]*job.Job, error) {
	if p.Limit <= 0 {
		return nil, nil
	}

	var (
		rows pgx.Rows
		err  error
	)
	if p.Aging.Enabled {
		rows, err = r.db.Query(ctx, claimAgingQuery,
			p.Limit, p.WorkerID, p.LeaseDuration.Seconds(),
			p.Aging.BoostPerMinute, p.Aging.MaxBoost)
	} else {
		rows, err = r.db.Query(ctx, claimQuery,
			p.Limit, p.WorkerID, p.LeaseDuration.Seconds())
	}
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	defer rows.Close()

	claimed := make([]*job.Job, 0, p.Limit)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan claimed job: %w", err)
		}
		claimed = append(claimed, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed jobs: %w", err)
	}

	return claimed, nil
}

// completeQuery is fenced on worker_id AND lease_epoch AND status.
//
// The three extra predicates are the whole point. A worker that was frozen
// past its lease expiry — a long GC pause, a suspended VM, a network partition
// — wakes up believing it still owns the job. By then the reaper has returned
// the job to pending and another worker has claimed it, bumping lease_epoch.
// This UPDATE then matches zero rows, and the stale worker learns it lost
// rather than silently overwriting the new owner's result.
//
// With a naive `WHERE id = $1` the stale worker would mark as completed a job
// that is currently executing somewhere else.
const completeQuery = `
	UPDATE jobs
	   SET status       = 'completed',
	       completed_at = now(),
	       lease_until  = NULL,
	       last_error   = NULL
	 WHERE id          = $1
	   AND worker_id   = $2
	   AND lease_epoch = $3
	   AND status      = 'running'`

// Complete marks a leased job finished, or reports ErrLeaseLost.
func (r *JobRepository) Complete(ctx context.Context, l job.Lease) error {
	tag, err := r.db.Exec(ctx, completeQuery, l.JobID, l.WorkerID, l.Epoch)
	if err != nil {
		return fmt.Errorf("complete job %s: %w", l.JobID, err)
	}
	if tag.RowsAffected() == 0 {
		// Either the lease expired and somebody else owns the job now, or the
		// job was cancelled underneath us. Both mean the same thing to the
		// caller: this worker's result is no longer authoritative.
		return fmt.Errorf("complete job %s: %w", l.JobID, job.ErrLeaseLost)
	}
	return nil
}

// failQuery records a failed attempt and picks the next state in one statement.
//
// Deciding retry-vs-dead-letter inside SQL, rather than reading the row into Go
// and writing it back, keeps the decision atomic with the write. Doing it in
// two round trips would leave a window in which the row's attempts value could
// change underneath the decision.
//
// available_at is left untouched when dead-lettering: the job is not going to
// become available again, and preserving the value keeps the forensic record.
const failQuery = `
	UPDATE jobs
	   SET status = CASE
	                  WHEN $4::boolean OR attempts > max_retries THEN 'dead_letter'
	                  ELSE 'pending'
	                END,
	       available_at = CASE
	                  WHEN $4::boolean OR attempts > max_retries THEN available_at
	                  ELSE now() + make_interval(secs => $5)
	                END,
	       failed_at   = now(),
	       last_error  = $6,
	       lease_until = NULL
	 WHERE id          = $1
	   AND worker_id   = $2
	   AND lease_epoch = $3
	   AND status      = 'running'
	RETURNING status`

// Fail records a failed attempt and returns the job's resulting status.
func (r *JobRepository) Fail(ctx context.Context, l job.Lease, f job.FailParams) (job.Status, error) {
	var status string

	err := r.db.QueryRow(ctx, failQuery,
		l.JobID,
		l.WorkerID,
		l.Epoch,
		f.Permanent,
		f.RetryDelay.Seconds(),
		job.TruncateError(f.Cause),
	).Scan(&status)

	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("fail job %s: %w", l.JobID, job.ErrLeaseLost)
	}
	if err != nil {
		return "", fmt.Errorf("fail job %s: %w", l.JobID, err)
	}

	return job.Status(status), nil
}

// renewLeaseQuery pushes the deadline out, fenced exactly like the others.
//
// The fencing predicate is what stops a worker taking its lease BACK. Once the
// reaper has requeued a job and somebody else has claimed it, the old worker's
// epoch is stale and this update matches nothing, so it cannot resurrect a
// lease it already lost.
const renewLeaseQuery = `
	UPDATE jobs
	   SET lease_until = now() + make_interval(secs => $4)
	 WHERE id          = $1
	   AND worker_id   = $2
	   AND lease_epoch = $3
	   AND status      = 'running'`

// RenewLease extends a lease that is still held by this worker.
func (r *JobRepository) RenewLease(ctx context.Context, l job.Lease, d time.Duration) error {
	tag, err := r.db.Exec(ctx, renewLeaseQuery, l.JobID, l.WorkerID, l.Epoch, d.Seconds())
	if err != nil {
		return fmt.Errorf("renew lease on job %s: %w", l.JobID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("renew lease on job %s: %w", l.JobID, job.ErrLeaseLost)
	}
	return nil
}

// reclaimQuery is crash recovery.
//
// A worker that is SIGKILLed, panics at the process level, loses its machine or
// is partitioned from the network never reaches Complete or Fail. Its jobs stay
// marked `running` with a lease that quietly expires, and without this query
// nothing in the system would ever look at them again.
//
// Three details matter:
//
//   - FOR UPDATE SKIP LOCKED in the inner select, so several worker processes
//     can run the reaper concurrently without fighting each other. The reaper
//     is idempotent by construction.
//
//   - LIMIT, so one pass after a mass outage is a short transaction rather than
//     an UPDATE over a hundred thousand rows that blocks the claim path.
//
//   - attempts is NOT incremented. The claim already counted this attempt, so
//     a job whose worker keeps dying still exhausts its budget and eventually
//     dead-letters instead of cycling forever.
const reclaimQuery = `
	WITH expired AS (
		SELECT id
		  FROM jobs
		 WHERE status = 'running'
		   AND lease_until < now()
		 ORDER BY lease_until ASC
		 FOR UPDATE SKIP LOCKED
		 LIMIT $1
	)
	UPDATE jobs AS j
	   SET status = CASE
	                  WHEN j.attempts > j.max_retries THEN 'dead_letter'
	                  ELSE 'pending'
	                END,
	       available_at = CASE
	                  WHEN j.attempts > j.max_retries THEN j.available_at
	                  ELSE now()
	                END,
	       lease_until = NULL,
	       failed_at   = now(),
	       last_error  = 'lease expired: worker ' || coalesce(j.worker_id, 'unknown') ||
	                     ' stopped reporting; job reclaimed'
	  FROM expired e
	 WHERE j.id = e.id
	RETURNING j.status`

// ReclaimExpired returns timed-out jobs to the queue.
func (r *JobRepository) ReclaimExpired(ctx context.Context, limit int) (job.ReclaimResult, error) {
	if limit <= 0 {
		limit = job.DefaultReclaimBatch
	}

	rows, err := r.db.Query(ctx, reclaimQuery, limit)
	if err != nil {
		return job.ReclaimResult{}, fmt.Errorf("reclaim expired leases: %w", err)
	}
	defer rows.Close()

	var result job.ReclaimResult
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			return job.ReclaimResult{}, fmt.Errorf("scan reclaimed job: %w", err)
		}
		if job.Status(status) == job.StatusDeadLetter {
			result.DeadLettered++
		} else {
			result.Requeued++
		}
	}
	if err := rows.Err(); err != nil {
		return job.ReclaimResult{}, fmt.Errorf("iterate reclaimed jobs: %w", err)
	}

	return result, nil
}

// retryDeadLetterQuery replays a dead-lettered job.
//
// attempts resets to zero, giving the job a full budget again. That is the
// semantic an operator wants: they have fixed the bug or the upstream outage
// has passed, and the job deserves a genuine fresh start rather than one last
// attempt. The status predicate means replaying a completed or running job is
// impossible rather than merely discouraged.
const retryDeadLetterQuery = `
	UPDATE jobs
	   SET status       = 'pending',
	       attempts     = 0,
	       available_at = now(),
	       lease_until  = NULL,
	       worker_id    = NULL,
	       failed_at    = NULL,
	       started_at   = NULL
	 WHERE id     = $1
	   AND status = 'dead_letter'
	RETURNING ` + jobColumns

// RetryDeadLetter returns a dead-lettered job to the queue.
func (r *JobRepository) RetryDeadLetter(ctx context.Context, id uuid.UUID) (*job.Job, error) {
	j, err := scanJob(r.db.QueryRow(ctx, retryDeadLetterQuery, id))
	if errors.Is(err, pgx.ErrNoRows) {
		// Zero rows means either the id does not exist or the job is not
		// dead-lettered. Distinguish them so the API can answer 404 versus 409,
		// which are genuinely different problems for the caller.
		existing, lookupErr := r.GetByID(ctx, id)
		if lookupErr != nil {
			return nil, lookupErr // ErrNotFound
		}
		return nil, &job.InvalidTransitionError{From: existing.Status, To: job.StatusPending}
	}
	if err != nil {
		return nil, fmt.Errorf("retry dead-lettered job %s: %w", id, err)
	}
	return j, nil
}
