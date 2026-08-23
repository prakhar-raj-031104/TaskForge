package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
	jobpg "github.com/anjani-kr-singh-ai/taskforge/internal/job/postgres"
)

// seedPending inserts n claimable jobs of the given type and returns their ids.
func seedPending(t *testing.T, pool *pgxpool.Pool, jobType string, n int, priority int) []uuid.UUID {
	t.Helper()

	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()
	now := time.Now().UTC()

	ids := make([]uuid.UUID, 0, n)
	for i := range n {
		j := &job.Job{
			ID:          uuid.New(),
			Type:        jobType,
			Payload:     json.RawMessage(`{}`),
			Status:      job.StatusPending,
			Priority:    priority,
			MaxRetries:  job.DefaultMaxRetries,
			AvailableAt: now.Add(-time.Duration(i) * time.Millisecond),
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if _, _, err := repo.Create(ctx, j); err != nil {
			t.Fatalf("seed job %d: %v", i, err)
		}
		ids = append(ids, j.ID)
	}
	return ids
}

// TestClaimNeverHandsTheSameJobToTwoWorkers is THE test for this system.
//
// Sixteen goroutines, each acting as a separate worker, hammer the claim query
// simultaneously against a pool of jobs. If FOR UPDATE SKIP LOCKED were
// replaced with a plain SELECT-then-UPDATE, this test would show the same job
// id claimed by several workers, and the total claimed count would exceed the
// number of jobs that exist.
func TestClaimNeverHandsTheSameJobToTwoWorkers(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	jobType := uniqueType(t)
	cleanup(t, pool, jobType)

	const (
		totalJobs = 200
		workers   = 16
		batchSize = 5
		leaseSecs = 60
		maxRounds = 200 // safety valve so a bug cannot hang the suite
	)

	seedPending(t, pool, jobType, totalJobs, job.DefaultPriority)

	var (
		mu        sync.Mutex
		claimed   = make(map[uuid.UUID]string) // job id -> the worker that got it
		dupes     []string
		ourIDs    []string
		ourIDsMux sync.Mutex
	)

	start := make(chan struct{})
	var wg sync.WaitGroup

	for w := range workers {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()

			repo := jobpg.NewJobRepository(pool)
			workerID := uuid.NewString()

			ourIDsMux.Lock()
			ourIDs = append(ourIDs, workerID)
			ourIDsMux.Unlock()

			// All goroutines block here so they hit the database together
			// rather than in whatever order they happened to be scheduled.
			<-start

			for range maxRounds {
				batch, err := repo.Claim(ctx, job.ClaimParams{
					WorkerID:      workerID,
					Limit:         batchSize,
					LeaseDuration: leaseSecs * time.Second,
				})
				if err != nil {
					t.Errorf("worker %d claim: %v", workerNum, err)
					return
				}
				if len(batch) == 0 {
					return // queue drained
				}

				mu.Lock()
				for _, j := range batch {
					if owner, seen := claimed[j.ID]; seen {
						dupes = append(dupes,
							"job "+j.ID.String()+" claimed by "+owner+" and again by "+workerID)
						continue
					}
					claimed[j.ID] = workerID
				}
				mu.Unlock()
			}
		}(w)
	}

	close(start)
	wg.Wait()

	// THE invariant. This assertion is unconditional: whatever else is going on,
	// no job may ever be handed to two claimants.
	if len(dupes) > 0 {
		t.Fatalf("%d job(s) were claimed more than once:\n%v", len(dupes), dupes)
	}

	// Completeness has to account for the fact that a real worker fleet may be
	// running against this same database - `docker compose up` starts workers
	// that poll every queue, including this test's jobs. Those are legitimate
	// competing claimants, and counting them is more useful than demanding an
	// exclusive database: it exercises exactly the property under test, with
	// genuinely independent processes rather than goroutines.
	var pending, takenByOthers, runningWithoutLease int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE status = 'pending'),
		       count(*) FILTER (WHERE worker_id IS NOT NULL AND NOT (worker_id = ANY($2))),
		       count(*) FILTER (WHERE status = 'running' AND (lease_until IS NULL OR worker_id IS NULL))
		  FROM jobs WHERE type = $1`, jobType, ourIDs).
		Scan(&pending, &takenByOthers, &runningWithoutLease); err != nil {
		t.Fatal(err)
	}

	if takenByOthers > 0 {
		t.Logf("%d of %d jobs were claimed by an external worker fleet running against "+
			"this database; that is expected when the Compose stack is up and is itself "+
			"evidence that SKIP LOCKED holds across processes", takenByOthers, totalJobs)
	}

	// Every seeded job must be accounted for exactly once: ours plus theirs.
	if got := len(claimed) + takenByOthers; got != totalJobs {
		t.Errorf("accounted for %d jobs (%d claimed here + %d taken elsewhere), want %d",
			got, len(claimed), takenByOthers, totalJobs)
	}
	if len(claimed) == 0 {
		t.Error("this test claimed nothing at all; the claim query is not working")
	}
	if pending != 0 {
		t.Errorf("%d jobs left pending", pending)
	}
	// The invariant that jobs_running_has_lease also enforces in the database.
	if runningWithoutLease != 0 {
		t.Errorf("%d running jobs have no lease or owner", runningWithoutLease)
	}
}

// TestClaimIncrementsAttemptsAndEpoch pins down the two counters the whole
// crash-recovery story depends on.
func TestClaimIncrementsAttemptsAndEpoch(t *testing.T) {
	pool := testPool(t)
	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()

	jobType := uniqueType(t)
	cleanup(t, pool, jobType)
	seedPending(t, pool, jobType, 1, job.DefaultPriority)

	first, err := repo.Claim(ctx, job.ClaimParams{
		WorkerID: "w1", Limit: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim: got %d jobs, err %v", len(first), err)
	}

	j := first[0]
	if j.Status != job.StatusRunning {
		t.Errorf("status = %q, want running", j.Status)
	}
	// Attempts increments at CLAIM time, not on failure, so a job that crashes
	// its worker still consumes its retry budget instead of looping forever.
	if j.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", j.Attempts)
	}
	if j.LeaseEpoch != 1 {
		t.Errorf("lease_epoch = %d, want 1", j.LeaseEpoch)
	}
	if j.LeaseUntil == nil {
		t.Fatal("lease_until is nil on a running job")
	}
	if j.WorkerID == nil || *j.WorkerID != "w1" {
		t.Errorf("worker_id = %v, want w1", j.WorkerID)
	}

	// Send it back to pending, as a retry would, and claim again.
	if _, err := repo.Fail(ctx, job.Lease{JobID: j.ID, WorkerID: "w1", Epoch: j.LeaseEpoch},
		job.FailParams{Cause: "boom", RetryDelay: 0}); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	second, err := repo.Claim(ctx, job.ClaimParams{
		WorkerID: "w2", Limit: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(second) != 1 {
		t.Fatalf("second claim: got %d jobs, err %v", len(second), err)
	}
	if second[0].Attempts != 2 {
		t.Errorf("attempts = %d, want 2", second[0].Attempts)
	}
	// The epoch moved, which is what invalidates the first worker's lease.
	if second[0].LeaseEpoch != 2 {
		t.Errorf("lease_epoch = %d, want 2", second[0].LeaseEpoch)
	}
}

// TestClaimRespectsAvailableAt proves scheduled jobs stay invisible.
func TestClaimRespectsAvailableAt(t *testing.T) {
	pool := testPool(t)
	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()

	jobType := uniqueType(t)
	cleanup(t, pool, jobType)

	now := time.Now().UTC()
	future := now.Add(time.Hour)

	if _, _, err := repo.Create(ctx, &job.Job{
		ID: uuid.New(), Type: jobType, Payload: json.RawMessage(`{}`),
		Status: job.StatusPending, Priority: job.DefaultPriority,
		MaxRetries: job.DefaultMaxRetries,
		// scheduled_at is user intent; available_at is what the claim reads.
		ScheduledAt: &future, AvailableAt: future,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := repo.Claim(ctx, job.ClaimParams{
		WorkerID: "w1", Limit: 10, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("claimed %d future-scheduled jobs, want 0", len(got))
	}
}

// TestClaimOrdersByPriority proves the ORDER BY and its index are doing work.
func TestClaimOrdersByPriority(t *testing.T) {
	pool := testPool(t)
	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()

	jobType := uniqueType(t)
	cleanup(t, pool, jobType)

	// Insert low priority FIRST, so insertion order and priority order differ.
	seedPending(t, pool, jobType, 5, 10)
	seedPending(t, pool, jobType, 5, 90)

	got, err := repo.Claim(ctx, job.ClaimParams{
		WorkerID: "w1", Limit: 5, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("claimed %d jobs, want 5", len(got))
	}
	for _, j := range got {
		if j.Priority != 90 {
			t.Errorf("claimed a priority %d job while priority 90 work was waiting", j.Priority)
		}
	}
}

// TestCompleteRejectsAStaleEpoch is the zombie-worker scenario from
// docs/design.md section 6, reproduced deterministically.
func TestCompleteRejectsAStaleEpoch(t *testing.T) {
	pool := testPool(t)
	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()

	jobType := uniqueType(t)
	cleanup(t, pool, jobType)
	seedPending(t, pool, jobType, 1, job.DefaultPriority)

	// Worker A claims the job and then "freezes".
	a, err := repo.Claim(ctx, job.ClaimParams{WorkerID: "worker-a", Limit: 1, LeaseDuration: time.Minute})
	if err != nil || len(a) != 1 {
		t.Fatalf("worker-a claim: %v", err)
	}
	staleLease := job.Lease{JobID: a[0].ID, WorkerID: "worker-a", Epoch: a[0].LeaseEpoch}

	// Its lease expires and the reaper returns the job to pending.
	if _, err := pool.Exec(ctx, `
		UPDATE jobs SET status = 'pending', lease_until = NULL, worker_id = NULL
		 WHERE id = $1`, a[0].ID); err != nil {
		t.Fatal(err)
	}

	// Worker B claims it, which bumps lease_epoch.
	b, err := repo.Claim(ctx, job.ClaimParams{WorkerID: "worker-b", Limit: 1, LeaseDuration: time.Minute})
	if err != nil || len(b) != 1 {
		t.Fatalf("worker-b claim: %v", err)
	}
	if b[0].LeaseEpoch <= staleLease.Epoch {
		t.Fatalf("epoch did not advance: %d then %d", staleLease.Epoch, b[0].LeaseEpoch)
	}

	// Worker A wakes up and tries to record success. It must be refused.
	err = repo.Complete(ctx, staleLease)
	if !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("stale Complete returned %v, want ErrLeaseLost", err)
	}

	// And it must have changed nothing: worker B still owns a running job.
	var status string
	var owner *string
	if err := pool.QueryRow(ctx, `SELECT status, worker_id FROM jobs WHERE id = $1`, a[0].ID).
		Scan(&status, &owner); err != nil {
		t.Fatal(err)
	}
	if status != string(job.StatusRunning) {
		t.Errorf("status = %q, want running; the stale worker clobbered the new owner", status)
	}
	if owner == nil || *owner != "worker-b" {
		t.Errorf("worker_id = %v, want worker-b", owner)
	}

	// Worker B, holding the current epoch, succeeds.
	if err := repo.Complete(ctx, job.Lease{JobID: b[0].ID, WorkerID: "worker-b", Epoch: b[0].LeaseEpoch}); err != nil {
		t.Errorf("worker-b Complete: %v", err)
	}
}

// TestFailRetriesUntilBudgetExhausted walks a job through its whole retry
// lifecycle and into the dead-letter queue.
func TestFailRetriesUntilBudgetExhausted(t *testing.T) {
	pool := testPool(t)
	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()

	jobType := uniqueType(t)
	cleanup(t, pool, jobType)

	const maxRetries = 2
	now := time.Now().UTC()
	id := uuid.New()
	if _, _, err := repo.Create(ctx, &job.Job{
		ID: id, Type: jobType, Payload: json.RawMessage(`{}`),
		Status: job.StatusPending, Priority: job.DefaultPriority,
		MaxRetries: maxRetries, AvailableAt: now, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// maxRetries=2 means three runs in total: the first attempt plus two retries.
	wantStatuses := []job.Status{job.StatusPending, job.StatusPending, job.StatusDeadLetter}

	for i, want := range wantStatuses {
		claimed, err := repo.Claim(ctx, job.ClaimParams{
			WorkerID: "w1", Limit: 1, LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatalf("attempt %d claim: %v", i+1, err)
		}
		if len(claimed) != 1 {
			t.Fatalf("attempt %d: claimed %d jobs, want 1", i+1, len(claimed))
		}

		got, err := repo.Fail(ctx,
			job.Lease{JobID: id, WorkerID: "w1", Epoch: claimed[0].LeaseEpoch},
			job.FailParams{Cause: "simulated failure", RetryDelay: 0})
		if err != nil {
			t.Fatalf("attempt %d fail: %v", i+1, err)
		}
		if got != want {
			t.Fatalf("attempt %d: status = %q, want %q", i+1, got, want)
		}
	}

	// It must stay dead-lettered and never be claimed again.
	leftover, err := repo.Claim(ctx, job.ClaimParams{WorkerID: "w1", Limit: 10, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range leftover {
		if j.ID == id {
			t.Error("a dead-lettered job was claimed again")
		}
	}
}

// TestFailPermanentSkipsRetries proves a permanent error goes straight to the
// dead-letter queue with its retry budget untouched.
func TestFailPermanentSkipsRetries(t *testing.T) {
	pool := testPool(t)
	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()

	jobType := uniqueType(t)
	cleanup(t, pool, jobType)
	seedPending(t, pool, jobType, 1, job.DefaultPriority)

	claimed, err := repo.Claim(ctx, job.ClaimParams{WorkerID: "w1", Limit: 1, LeaseDuration: time.Minute})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v", err)
	}

	got, err := repo.Fail(ctx,
		job.Lease{JobID: claimed[0].ID, WorkerID: "w1", Epoch: claimed[0].LeaseEpoch},
		job.FailParams{Cause: "malformed payload", Permanent: true, RetryDelay: time.Hour})
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if got != job.StatusDeadLetter {
		t.Errorf("status = %q, want dead_letter on the first permanent failure", got)
	}
}

// TestFailSetsBackoffDelay proves available_at moves into the future so the
// retry is not claimed immediately.
func TestFailSetsBackoffDelay(t *testing.T) {
	pool := testPool(t)
	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()

	jobType := uniqueType(t)
	cleanup(t, pool, jobType)
	seedPending(t, pool, jobType, 1, job.DefaultPriority)

	claimed, err := repo.Claim(ctx, job.ClaimParams{WorkerID: "w1", Limit: 1, LeaseDuration: time.Minute})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v", err)
	}

	if _, err := repo.Fail(ctx,
		job.Lease{JobID: claimed[0].ID, WorkerID: "w1", Epoch: claimed[0].LeaseEpoch},
		job.FailParams{Cause: "transient", RetryDelay: 30 * time.Second}); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	// It is pending again, but not yet claimable.
	again, err := repo.Claim(ctx, job.ClaimParams{WorkerID: "w1", Limit: 10, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range again {
		if j.ID == claimed[0].ID {
			t.Error("a job in retry backoff was claimed before its delay elapsed")
		}
	}

	stored, err := repo.GetByID(ctx, claimed[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != job.StatusPending {
		t.Errorf("status = %q, want pending", stored.Status)
	}
	if !stored.AvailableAt.After(time.Now()) {
		t.Errorf("available_at = %s, want a time in the future", stored.AvailableAt)
	}
	// scheduled_at is user intent and must never be touched by a retry.
	if stored.ScheduledAt != nil {
		t.Errorf("scheduled_at = %v, want nil; retries must not write to it", stored.ScheduledAt)
	}
}
