// Package worker runs jobs. It claims work from the queue, dispatches it to
// registered handlers with bounded concurrency, records the outcome, and drains
// cleanly on shutdown.
//
// The worker core knows nothing about any specific job type. Adding one means
// registering a handler in cmd/worker; nothing in this package changes.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/logging"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/metrics"
)

// writeTimeout bounds the database write that records a job's outcome.
//
// Short and independent of the job's own timeout: by the time we get here the
// work is already done, and all that remains is one small UPDATE. If that
// cannot complete quickly the lease will expire and the job will be retried,
// which is the correct at-least-once behaviour.
const writeTimeout = 5 * time.Second

// minLeaseDuration is the shortest lease that leaves room for renewals to
// survive a transient hiccup. Renewal runs at LeaseDuration/3, so this allows
// two consecutive failures before a healthy job is reclaimed.
const minLeaseDuration = 5 * time.Second

// leaseRenewalDivisor sets the renewal interval as LeaseDuration/divisor.
//
// Three, not two: renewing at half the lease leaves no margin at all, because a
// single slow renewal lands exactly at expiry. At a third, two consecutive
// renewals can fail before the job is at risk.
const leaseRenewalDivisor = 3

// Config describes one worker process.
type Config struct {
	// ID identifies this process across the fleet.
	ID string

	// Concurrency is how many jobs this process runs at once, i.e. the maximum
	// number of goroutines executing handlers.
	Concurrency int

	// PollInterval is how long to wait before asking again when the queue was
	// empty. It does NOT delay work when jobs are available.
	PollInterval time.Duration

	// LeaseDuration is how long a claim is valid for.
	LeaseDuration time.Duration

	// JobTimeout is the maximum a single handler may run.
	JobTimeout time.Duration

	// ShutdownTimeout bounds how long Run waits for in-flight jobs after the
	// context is cancelled.
	ShutdownTimeout time.Duration

	// Backoff computes the delay before a failed job becomes claimable again.
	Backoff job.Backoff

	// Aging optionally lets long-waiting jobs overtake newer high-priority
	// work, preventing starvation. Off by default; see job.AgingPolicy for the
	// index tradeoff it makes.
	Aging job.AgingPolicy

	// Metrics collects job counters and histograms. When nil, New installs a
	// private unscraped registry so the hot path never has to nil-check.
	Metrics *metrics.Metrics
}

func (c Config) validate() error {
	var errs []error
	if c.ID == "" {
		errs = append(errs, errors.New("worker id must not be empty"))
	}
	if c.Concurrency < 1 {
		errs = append(errs, fmt.Errorf("concurrency must be at least 1, got %d", c.Concurrency))
	}
	if c.PollInterval <= 0 {
		errs = append(errs, errors.New("poll interval must be positive"))
	}
	if c.LeaseDuration <= 0 {
		errs = append(errs, errors.New("lease duration must be positive"))
	}
	if c.JobTimeout <= 0 {
		errs = append(errs, errors.New("job timeout must be positive"))
	}
	// JobTimeout is deliberately NOT required to be shorter than LeaseDuration.
	// While a job runs the worker renews its lease every LeaseDuration/3, so a
	// ten-minute job is safe under a one-minute lease. That decoupling is the
	// point: a short lease means a crashed worker is detected in seconds, while
	// long jobs still run to completion.
	//
	// The lease must simply be long enough that a couple of missed renewals -
	// a slow query, a brief network blip - do not expire it.
	if c.LeaseDuration > 0 && c.LeaseDuration < minLeaseDuration {
		errs = append(errs, fmt.Errorf(
			"lease duration must be at least %s so a transient database blip does not expire a healthy job's lease, got %s",
			minLeaseDuration, c.LeaseDuration))
	}
	return errors.Join(errs...)
}

// Worker claims and executes jobs.
type Worker struct {
	cfg      Config
	queue    job.Queue
	registry *job.Registry
	log      *slog.Logger

	// inFlight counts jobs currently executing. Read by the heartbeat loop from
	// another goroutine, so it is atomic rather than a plain int: a plain int
	// here would be a data race the detector would catch, and an incorrect
	// value on a dashboard even when it did not.
	inFlight atomic.Int64
}

// ActiveJobs reports how many jobs this worker is running right now. Safe to
// call from any goroutine.
func (w *Worker) ActiveJobs() int { return int(w.inFlight.Load()) }

// New builds a worker, rejecting an unworkable configuration at startup.
func New(cfg Config, queue job.Queue, registry *job.Registry, log *slog.Logger) (*Worker, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("worker config: %w", err)
	}
	if queue == nil {
		return nil, errors.New("worker: queue must not be nil")
	}
	if registry == nil {
		return nil, errors.New("worker: registry must not be nil")
	}
	if cfg.Metrics == nil {
		// A private registry nobody scrapes. Recording into it is a few atomic
		// adds, which is cheaper than a nil check on every job and keeps the
		// instrumentation code free of branches.
		cfg.Metrics = metrics.New()
	}

	return &Worker{
		cfg:      cfg,
		queue:    queue,
		registry: registry,
		log:      log.With("worker_id", cfg.ID),
	}, nil
}

// Run claims and executes jobs until ctx is cancelled, then drains.
//
// # The concurrency model
//
// `free` is a counting semaphore: a buffered channel holding one token per
// execution slot. The loop cannot claim work without first holding a token, so
// the number of jobs in flight can never exceed Concurrency. The bound is
// structural rather than something a future edit could accidentally break.
//
// It also claims exactly as many jobs as there are free slots. Claiming a fixed
// batch would mark jobs `running` while they sat in a queue inside this
// process, burning lease time before execution even started.
func (w *Worker) Run(ctx context.Context) error {
	free := make(chan struct{}, w.cfg.Concurrency)
	for range w.cfg.Concurrency {
		free <- struct{}{}
	}

	var wg sync.WaitGroup

	w.log.Info("worker started",
		"concurrency", w.cfg.Concurrency,
		"poll_interval", w.cfg.PollInterval,
		"lease_duration", w.cfg.LeaseDuration,
		"job_timeout", w.cfg.JobTimeout,
		"job_types", w.registry.Types(),
	)

	for {
		// Block until at least one slot is free. This is the backpressure: a
		// saturated worker stops asking the database for work entirely, instead
		// of polling pointlessly while it has nowhere to put the results.
		select {
		case <-ctx.Done():
			return w.drain(&wg)
		case <-free:
		}

		// Opportunistically collect any other slots that are free right now, so
		// one round trip can fill the whole worker instead of one job per poll.
		slots := 1
	collect:
		for {
			select {
			case <-free:
				slots++
			default:
				break collect
			}
		}

		claimed, err := w.queue.Claim(ctx, job.ClaimParams{
			WorkerID:      w.cfg.ID,
			Limit:         slots,
			LeaseDuration: w.cfg.LeaseDuration,
			Aging:         w.cfg.Aging,
		})
		if err != nil {
			release(free, slots)

			if ctx.Err() != nil {
				return w.drain(&wg)
			}
			// A failed claim is usually a transient database problem. Log it and
			// back off by one poll interval rather than spinning.
			w.log.Error("claiming jobs failed", "error", err)
			if !sleep(ctx, w.cfg.PollInterval) {
				return w.drain(&wg)
			}
			continue
		}

		// Hand back the slots we asked for but did not fill.
		release(free, slots-len(claimed))

		if len(claimed) == 0 {
			// Empty queue: wait before asking again. Milestone 10 replaces this
			// with LISTEN/NOTIFY so an idle worker wakes on insert instead of on
			// a timer, but polling stays as the backstop, because a missed
			// notification must never mean a lost job.
			if !sleep(ctx, w.cfg.PollInterval) {
				return w.drain(&wg)
			}
			continue
		}

		w.log.Debug("claimed jobs", "count", len(claimed), "slots_requested", slots)

		// Queue latency: how long the job sat available before anyone took it.
		// Recorded here, at claim time, because it is the only moment both
		// numbers are known. This is the metric users actually feel; processing
		// duration is invisible to them by comparison.
		claimedAt := time.Now()
		for _, j := range claimed {
			if wait := claimedAt.Sub(j.AvailableAt); wait > 0 {
				w.cfg.Metrics.JobWait.WithLabelValues(j.Type).Observe(wait.Seconds())
			}
		}

		for _, j := range claimed {
			wg.Add(1)
			w.inFlight.Add(1)
			go func(j *job.Job) {
				defer wg.Done()
				// Return the slot however this goroutine ends. Without this the
				// worker would silently lose capacity job by job until it
				// stalled with nothing in flight.
				defer func() { free <- struct{}{} }()
				defer w.inFlight.Add(-1)

				w.process(ctx, j)
			}(j)
		}
	}
}

// process executes one claimed job and records the outcome.
func (w *Worker) process(ctx context.Context, j *job.Job) {
	lease := job.Lease{
		JobID:    j.ID,
		WorkerID: w.cfg.ID,
		// The epoch handed out by the claim. Every write this worker makes
		// carries it, so a stale worker cannot overwrite a newer owner's work.
		Epoch: j.LeaseEpoch,
	}

	log := w.log.With(
		"job_id", j.ID,
		"job_type", j.Type,
		"attempt", j.Attempts,
		"max_retries", j.MaxRetries,
		"priority", j.Priority,
		// The payload is never logged: it routinely holds email addresses,
		// tokens and customer data.
	)

	handler, err := w.registry.Get(j.Type)
	if err != nil {
		// Permanent by construction, so this dead-letters on the first attempt.
		w.recordFailure(ctx, log, j, lease, err)
		return
	}

	// context.WithoutCancel keeps the context's VALUES (request ids, trace
	// spans) while discarding its cancellation. That is what allows an
	// in-flight job to finish after SIGTERM instead of being killed mid-write,
	// which is the whole point of graceful shutdown. The job is still bounded,
	// by its own JobTimeout.
	jobCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.JobTimeout)
	defer cancel()
	jobCtx = logging.WithLogger(jobCtx, log)

	// Keep the lease alive for as long as the handler runs. This is what lets
	// the lease be short (fast crash detection) without a slow-but-healthy job
	// being reclaimed and executed twice.
	stopRenewal := w.renewLeaseInBackground(jobCtx, lease, log)

	start := time.Now()
	err = w.safeHandle(jobCtx, handler, j)
	elapsed := time.Since(start)

	// Stop renewing before writing the outcome, so the renewal goroutine cannot
	// push the lease out again after the job is already finished.
	stopRenewal()

	log = log.With("duration_ms", float64(elapsed.Microseconds())/1000)

	if err != nil {
		w.recordFailure(ctx, log, j, lease, err)
		return
	}

	// The write that records success also uses a detached context. If it used
	// the cancelled shutdown context, work that genuinely succeeded would be
	// recorded as failed and then run a second time.
	writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
	defer cancelWrite()

	if err := w.queue.Complete(writeCtx, lease); err != nil {
		if errors.Is(err, job.ErrLeaseLost) {
			// The fencing token did its job. The side effects of this run
			// already happened, and another worker is running the job again,
			// which is exactly why handlers must be idempotent.
			log.Warn("job finished but its lease had already expired; another worker now owns it, so this job's side effects may occur twice",
				"error", err)
			return
		}
		log.Error("recording job completion failed; the lease will expire and the job will be retried",
			"error", err)
		return
	}

	w.cfg.Metrics.JobsProcessed.WithLabelValues(j.Type, metrics.OutcomeSuccess).Inc()
	w.cfg.Metrics.JobDuration.WithLabelValues(j.Type, metrics.OutcomeSuccess).Observe(elapsed.Seconds())

	log.Info("job completed")
}

// renewLeaseInBackground keeps a lease alive while its job runs.
//
// It returns a stop function that cancels the renewal loop and waits for it to
// exit. Waiting matters: without it, a renewal could still be in flight when
// the caller writes the job's final state, and the two writes would race.
func (w *Worker) renewLeaseInBackground(ctx context.Context, lease job.Lease, log *slog.Logger) (stop func()) {
	interval := w.cfg.LeaseDuration / leaseRenewalDivisor
	if interval < time.Second {
		interval = time.Second
	}

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			// Detached from ctx so a renewal already in flight is not cancelled
			// by the handler finishing; bounded by its own short timeout.
			renewCtx, cancelRenew := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
			err := w.queue.RenewLease(renewCtx, lease, w.cfg.LeaseDuration)
			cancelRenew()

			if err == nil {
				log.Debug("lease renewed", "lease_duration", w.cfg.LeaseDuration)
				continue
			}

			if errors.Is(err, job.ErrLeaseLost) {
				// The job was reclaimed while we were still working on it.
				// Renewing again is pointless: the epoch has moved on and every
				// future write from this worker will be refused too.
				log.Warn("lease lost while the job was still running; another worker has taken it and this attempt's result will be discarded",
					"error", err)
				return
			}

			// A transient failure. Keep trying: there are two more renewals
			// before the lease actually expires.
			log.Error("renewing lease failed", "error", err)
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

// safeHandle calls a handler, converting a panic into an error.
//
// A panicking handler must not take down the worker process and every other job
// running inside it. The named return value is what lets the deferred closure
// replace the function's result: this is the one place in Go where a deferred
// function can change what the caller sees.
func (w *Worker) safeHandle(ctx context.Context, h job.Handler, j *job.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// Not marked permanent: a panic is usually a bug rather than bad
			// data, and a deploy may well fix it, so the job keeps its retries.
			err = fmt.Errorf("handler panicked: %v\n%s", r, debug.Stack())
		}
	}()

	return h.Handle(ctx, j)
}

// recordFailure writes the failure and logs the resulting state.
func (w *Worker) recordFailure(ctx context.Context, log *slog.Logger, j *job.Job, lease job.Lease, cause error) {
	permanent := job.IsPermanent(cause)

	// j.Attempts already includes this attempt, because the claim incremented
	// it. Attempt 1 therefore gets the first backoff step.
	delay := w.cfg.Backoff.Delay(j.Attempts)

	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
	defer cancel()

	status, err := w.queue.Fail(writeCtx, lease, job.FailParams{
		Cause:      cause.Error(),
		RetryDelay: delay,
		Permanent:  permanent,
	})
	if err != nil {
		if errors.Is(err, job.ErrLeaseLost) {
			log.Warn("job failed but its lease had already expired; another worker owns it now", "error", err)
			return
		}
		log.Error("recording job failure failed; the lease will expire and the job will be retried",
			"error", err, "cause", cause)
		return
	}

	switch status {
	case job.StatusDeadLetter:
		reason := metrics.ReasonRetriesExhausted
		if permanent {
			reason = metrics.ReasonPermanent
		}
		w.cfg.Metrics.JobsDeadLettered.WithLabelValues(j.Type, reason).Inc()
		w.cfg.Metrics.JobsProcessed.WithLabelValues(j.Type, metrics.OutcomeDeadLetter).Inc()

		log.Error("job dead-lettered",
			"error", cause,
			"permanent", permanent,
			"attempts_used", j.Attempts,
		)
	default:
		w.cfg.Metrics.JobsRetried.WithLabelValues(j.Type).Inc()
		w.cfg.Metrics.JobsProcessed.WithLabelValues(j.Type, metrics.OutcomeFailure).Inc()

		log.Warn("job failed, scheduled for retry",
			"error", cause,
			"retry_in", delay,
			"attempts_used", j.Attempts,
			"attempts_remaining", j.MaxRetries+1-j.Attempts,
		)
	}
}

// drain waits for in-flight jobs, bounded by ShutdownTimeout.
func (w *Worker) drain(wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	w.log.Info("worker stopping, waiting for in-flight jobs",
		"timeout", w.cfg.ShutdownTimeout)

	timer := time.NewTimer(w.cfg.ShutdownTimeout)
	defer timer.Stop()

	select {
	case <-done:
		w.log.Info("worker stopped cleanly, all in-flight jobs finished")
	case <-timer.C:
		// Abandoning jobs is safe, not a data loss event: their leases expire
		// and another worker reclaims them. That is at-least-once delivery
		// doing exactly what it promises.
		w.log.Warn("shutdown timeout exceeded; abandoning in-flight jobs. Their leases will expire and another worker will reclaim them")
	}

	return nil
}

// release hands n tokens back to the semaphore.
func release(free chan<- struct{}, n int) {
	for range n {
		free <- struct{}{}
	}
}

// sleep waits for d, returning false if ctx was cancelled first.
//
// time.Sleep would be wrong here: it cannot be interrupted, so a worker with a
// 30-second poll interval would ignore SIGTERM for up to 30 seconds and get
// SIGKILLed instead of shutting down.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	// Stopping the timer releases it immediately instead of leaving it armed
	// until it fires. On a loop that runs continuously, the difference is a
	// steady leak of runtime timers.
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
