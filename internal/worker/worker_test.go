package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeQueue is an in-memory job.Queue.
//
// The worker's concurrency, panic handling and shutdown behaviour are pure Go
// concerns, so they are tested here with no database at all. The SQL that backs
// the real Queue is tested separately against real PostgreSQL.
type fakeQueue struct {
	mu sync.Mutex

	pending   []*job.Job
	completed []uuid.UUID
	failures  []job.FailParams

	claimErr   error
	renewErr   error
	reclaimErr error

	// expired is how many jobs ReclaimExpired should report as recoverable.
	expired int

	// claimCalls counts round trips, so a test can assert the worker is not
	// hammering the database when the queue is empty.
	claimCalls atomic.Int64
	renewals   atomic.Int64
}

func newFakeQueue(jobs ...*job.Job) *fakeQueue {
	return &fakeQueue{pending: jobs}
}

func (q *fakeQueue) Claim(_ context.Context, p job.ClaimParams) ([]*job.Job, error) {
	q.claimCalls.Add(1)

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.claimErr != nil {
		return nil, q.claimErr
	}
	n := min(p.Limit, len(q.pending))
	batch := q.pending[:n]
	q.pending = q.pending[n:]

	for _, j := range batch {
		j.Attempts++
		j.LeaseEpoch++
		j.Status = job.StatusRunning
	}
	return batch, nil
}

func (q *fakeQueue) Complete(_ context.Context, l job.Lease) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.completed = append(q.completed, l.JobID)
	return nil
}

func (q *fakeQueue) Fail(_ context.Context, _ job.Lease, f job.FailParams) (job.Status, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.failures = append(q.failures, f)
	return job.StatusPending, nil
}

func (q *fakeQueue) RenewLease(_ context.Context, _ job.Lease, _ time.Duration) error {
	q.renewals.Add(1)
	return q.renewErr
}

func (q *fakeQueue) ReclaimExpired(_ context.Context, limit int) (job.ReclaimResult, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.reclaimErr != nil {
		return job.ReclaimResult{}, q.reclaimErr
	}
	n := min(limit, q.expired)
	q.expired -= n
	return job.ReclaimResult{Requeued: n}, nil
}

func (q *fakeQueue) completedIDs() []uuid.UUID {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]uuid.UUID(nil), q.completed...)
}

func (q *fakeQueue) recordedFailures() []job.FailParams {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]job.FailParams(nil), q.failures...)
}

func newJob(jobType string) *job.Job {
	return &job.Job{
		ID:         uuid.New(),
		Type:       jobType,
		Payload:    json.RawMessage(`{}`),
		Status:     job.StatusPending,
		Priority:   job.DefaultPriority,
		MaxRetries: job.DefaultMaxRetries,
	}
}

func testConfig() Config {
	return Config{
		ID:              "test-worker",
		Concurrency:     4,
		PollInterval:    5 * time.Millisecond,
		LeaseDuration:   time.Minute,
		JobTimeout:      2 * time.Second,
		ShutdownTimeout: 2 * time.Second,
		Backoff:         job.Backoff{Base: time.Millisecond, Max: time.Second, Factor: 2},
	}
}

// runUntilDrained runs the worker until the queue empties or the deadline hits.
func runUntilDrained(t *testing.T, w *Worker, q *fakeQueue, want int) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	deadline := time.After(5 * time.Second)
	for {
		if len(q.completedIDs())+len(q.recordedFailures()) >= want {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatalf("timed out: %d of %d jobs finished", len(q.completedIDs())+len(q.recordedFailures()), want)
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestWorkerProcessesJobs(t *testing.T) {
	t.Parallel()

	const total = 20
	jobs := make([]*job.Job, total)
	for i := range jobs {
		jobs[i] = newJob("noop")
	}
	q := newFakeQueue(jobs...)

	var handled atomic.Int64
	registry := job.NewRegistry()
	if err := registry.Register("noop", job.HandlerFunc(func(context.Context, *job.Job) error {
		handled.Add(1)
		return nil
	})); err != nil {
		t.Fatal(err)
	}

	w, err := New(testConfig(), q, registry, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	runUntilDrained(t, w, q, total)

	if got := handled.Load(); got != total {
		t.Errorf("handled %d jobs, want %d", got, total)
	}
	if got := len(q.completedIDs()); got != total {
		t.Errorf("completed %d jobs, want %d", got, total)
	}
}

// TestWorkerRespectsConcurrencyLimit is the reason the semaphore exists. It
// tracks how many handlers are running simultaneously and asserts the ceiling
// is never breached.
func TestWorkerRespectsConcurrencyLimit(t *testing.T) {
	t.Parallel()

	const (
		total       = 40
		concurrency = 3
	)

	jobs := make([]*job.Job, total)
	for i := range jobs {
		jobs[i] = newJob("slow")
	}
	q := newFakeQueue(jobs...)

	var (
		mu      sync.Mutex
		current int
		peak    int
	)

	registry := job.NewRegistry()
	if err := registry.Register("slow", job.HandlerFunc(func(context.Context, *job.Job) error {
		mu.Lock()
		current++
		if current > peak {
			peak = current
		}
		mu.Unlock()

		time.Sleep(10 * time.Millisecond)

		mu.Lock()
		current--
		mu.Unlock()
		return nil
	})); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	cfg.Concurrency = concurrency

	w, err := New(cfg, q, registry, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	runUntilDrained(t, w, q, total)

	mu.Lock()
	defer mu.Unlock()
	if peak > concurrency {
		t.Errorf("peak concurrency was %d, above the configured limit of %d", peak, concurrency)
	}
	if peak < 2 {
		t.Errorf("peak concurrency was %d; the worker is not running jobs in parallel at all", peak)
	}
}

// TestWorkerSurvivesAPanickingHandler: one bad handler must not take down the
// process and every other job running inside it.
func TestWorkerSurvivesAPanickingHandler(t *testing.T) {
	t.Parallel()

	q := newFakeQueue(newJob("boom"), newJob("fine"))

	registry := job.NewRegistry()
	if err := registry.Register("boom", job.HandlerFunc(func(context.Context, *job.Job) error {
		panic("handler exploded")
	})); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("fine", job.HandlerFunc(func(context.Context, *job.Job) error {
		return nil
	})); err != nil {
		t.Fatal(err)
	}

	w, err := New(testConfig(), q, registry, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	runUntilDrained(t, w, q, 2)

	failures := q.recordedFailures()
	if len(failures) != 1 {
		t.Fatalf("recorded %d failures, want 1", len(failures))
	}
	if failures[0].Cause == "" {
		t.Error("the panic was not recorded as a failure cause")
	}
	// A panic is usually a bug that a deploy can fix, so the job keeps its
	// retries rather than being dead-lettered immediately.
	if failures[0].Permanent {
		t.Error("a panic was marked permanent; it should stay retryable")
	}
	if len(q.completedIDs()) != 1 {
		t.Errorf("completed %d jobs, want the healthy one to still succeed", len(q.completedIDs()))
	}
}

// TestWorkerDeadLettersUnknownJobTypes proves an unregistered type fails
// permanently on the first attempt.
func TestWorkerDeadLettersUnknownJobTypes(t *testing.T) {
	t.Parallel()

	q := newFakeQueue(newJob("never_registered"))

	w, err := New(testConfig(), q, job.NewRegistry(), discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	runUntilDrained(t, w, q, 1)

	failures := q.recordedFailures()
	if len(failures) != 1 {
		t.Fatalf("recorded %d failures, want 1", len(failures))
	}
	if !failures[0].Permanent {
		t.Error("an unknown job type must be recorded as a permanent failure")
	}
}

// TestWorkerAppliesBackoffOnFailure checks the retry delay reaches the queue.
func TestWorkerAppliesBackoffOnFailure(t *testing.T) {
	t.Parallel()

	j := newJob("failing")
	q := newFakeQueue(j)

	registry := job.NewRegistry()
	if err := registry.Register("failing", job.HandlerFunc(func(context.Context, *job.Job) error {
		return errors.New("upstream unavailable")
	})); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	cfg.Backoff = job.Backoff{Base: 4 * time.Second, Max: time.Minute, Factor: 2}

	w, err := New(cfg, q, registry, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	runUntilDrained(t, w, q, 1)

	failures := q.recordedFailures()
	if len(failures) != 1 {
		t.Fatalf("recorded %d failures, want 1", len(failures))
	}
	// Attempt 1 with base 4s and equal jitter lands in [2s, 4s].
	if d := failures[0].RetryDelay; d < 2*time.Second || d > 4*time.Second {
		t.Errorf("retry delay = %s, want it within [2s, 4s]", d)
	}
	if failures[0].Permanent {
		t.Error("a plain error was marked permanent")
	}
}

// TestWorkerFinishesInFlightJobsAfterCancellation is graceful shutdown: a job
// already running when SIGTERM arrives must be allowed to complete, not killed
// halfway through its side effects.
func TestWorkerFinishesInFlightJobsAfterCancellation(t *testing.T) {
	t.Parallel()

	q := newFakeQueue(newJob("slow"))

	started := make(chan struct{})
	var finished atomic.Bool

	registry := job.NewRegistry()
	if err := registry.Register("slow", job.HandlerFunc(func(ctx context.Context, _ *job.Job) error {
		close(started)
		// Long enough that cancellation definitely lands mid-flight. The
		// handler deliberately does NOT watch ctx here, standing in for work
		// that cannot be interrupted safely.
		time.Sleep(300 * time.Millisecond)
		finished.Store(true)
		return nil
	})); err != nil {
		t.Fatal(err)
	}

	w, err := New(testConfig(), q, registry, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	<-started
	cancel() // SIGTERM arrives while the job is running

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}

	if !finished.Load() {
		t.Error("the in-flight job was abandoned instead of being allowed to finish")
	}
	if len(q.completedIDs()) != 1 {
		t.Error("the completion was not recorded; the write used the cancelled context")
	}
}

func TestWorkerStopsClaimingAfterCancellation(t *testing.T) {
	t.Parallel()

	q := newFakeQueue() // permanently empty

	w, err := New(testConfig(), q, job.NewRegistry(), discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run ignored cancellation; the poll sleep is not interruptible")
	}

	before := q.claimCalls.Load()
	time.Sleep(50 * time.Millisecond)
	if after := q.claimCalls.Load(); after != before {
		t.Errorf("the worker kept claiming after Run returned: %d then %d", before, after)
	}
}

// TestWorkerAllowsJobsLongerThanTheLease documents the decoupling that lease
// renewal buys: a ten-minute job under a one-minute lease is fine, because the
// worker renews every 20 seconds while it works. Without renewal this would be
// a duplicate-execution bug; with renewal it is the whole point, because a
// short lease means a crashed worker is detected in seconds.
func TestWorkerAllowsJobsLongerThanTheLease(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.JobTimeout = 10 * time.Minute
	cfg.LeaseDuration = time.Minute

	if _, err := New(cfg, newFakeQueue(), job.NewRegistry(), discardLogger()); err != nil {
		t.Fatalf("New rejected a long job under a renewable lease: %v", err)
	}
}

// TestWorkerRenewsTheLeaseWhileWorking is the mechanism that makes the above
// safe.
func TestWorkerRenewsTheLeaseWhileWorking(t *testing.T) {
	t.Parallel()

	q := newFakeQueue(newJob("slow"))

	registry := job.NewRegistry()
	if err := registry.Register("slow", job.HandlerFunc(func(ctx context.Context, _ *job.Job) error {
		// Long enough for several renewals at LeaseDuration/3 = ~1.6s floored
		// to the 1s minimum.
		time.Sleep(2500 * time.Millisecond)
		return nil
	})); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	cfg.LeaseDuration = 5 * time.Second // renew every ~1.6s

	w, err := New(cfg, q, registry, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	runUntilDrained(t, w, q, 1)

	if got := q.renewals.Load(); got < 1 {
		t.Errorf("the lease was renewed %d times during a 2.5s job; want at least 1", got)
	}
}

// TestWorkerRejectsATooShortLease: a lease shorter than a couple of renewal
// cycles would let an ordinary database hiccup reclaim a healthy job.
func TestWorkerRejectsATooShortLease(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.LeaseDuration = time.Second

	if _, err := New(cfg, newFakeQueue(), job.NewRegistry(), discardLogger()); err == nil {
		t.Fatal("New accepted a 1s lease")
	}
}

func TestReaperReclaimsUntilDrained(t *testing.T) {
	t.Parallel()

	q := newFakeQueue()
	q.expired = 250 // more than one batch of 100

	r := NewReaper(q, 5*time.Millisecond, 100, nil, discardLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	deadline := time.After(3 * time.Second)
	for {
		q.mu.Lock()
		remaining := q.expired
		q.mu.Unlock()
		if remaining == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("reaper left %d expired jobs unreclaimed", remaining)
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Reaper.Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Reaper.Run ignored cancellation")
	}
}

func TestReaperSurvivesQueueErrors(t *testing.T) {
	t.Parallel()

	q := newFakeQueue()
	q.reclaimErr = errors.New("database unavailable")

	r := NewReaper(q, 5*time.Millisecond, 100, nil, discardLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// A failing reclaim must not kill the loop; it logs and tries again later.
	if err := r.Run(ctx); err != nil {
		t.Errorf("Reaper.Run returned %v; a transient failure should not stop it", err)
	}
}

func TestWorkerConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"empty id", func(c *Config) { c.ID = "" }},
		{"zero concurrency", func(c *Config) { c.Concurrency = 0 }},
		{"negative concurrency", func(c *Config) { c.Concurrency = -1 }},
		{"zero poll interval", func(c *Config) { c.PollInterval = 0 }},
		{"zero lease", func(c *Config) { c.LeaseDuration = 0 }},
		{"zero job timeout", func(c *Config) { c.JobTimeout = 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig()
			tt.mutate(&cfg)

			if _, err := New(cfg, newFakeQueue(), job.NewRegistry(), discardLogger()); err == nil {
				t.Errorf("New accepted an invalid config: %s", tt.name)
			}
		})
	}

	if _, err := New(testConfig(), nil, job.NewRegistry(), discardLogger()); err == nil {
		t.Error("New accepted a nil queue")
	}
	if _, err := New(testConfig(), newFakeQueue(), nil, discardLogger()); err == nil {
		t.Error("New accepted a nil registry")
	}
}
