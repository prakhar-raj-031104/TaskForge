package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/metrics"
)

// Reaper returns jobs with expired leases to the queue.
//
// # Why this is the most important background loop in the system
//
// Every other failure path is cooperative: a handler returns an error, a worker
// records it, the job is retried. Crash recovery is the case where nothing
// cooperates. A worker that is SIGKILLed by the OOM killer, loses its machine,
// or is partitioned from the database never gets to say anything at all. Its
// jobs simply stay marked `running` with a lease that silently expires, and
// without this loop they would sit there forever - invisible to the queue,
// invisible to queue-depth metrics, and never retried.
//
// # Why every worker runs one
//
// Running the reaper on a single designated node would need leader election,
// and would make that node a single point of failure for the very mechanism
// that exists to survive failure. Instead every worker runs it, and the query
// is made safe for concurrent execution with FOR UPDATE SKIP LOCKED. The cost
// is one small indexed query per worker per interval; the benefit is that
// recovery keeps working as long as any worker is alive.
//
// If profiling ever showed that query load mattered, the right fix would be a
// PostgreSQL advisory lock (pg_try_advisory_lock) so only one worker reaps at a
// time - which is the concrete use case that would justify introducing advisory
// locks to this project. There is no evidence for it yet, so it is not here.
type Reaper struct {
	queue    job.Queue
	interval time.Duration
	batch    int
	metrics  *metrics.Metrics
	log      *slog.Logger
}

// NewReaper builds a reaper. m may be nil, in which case a private unscraped
// registry is used.
func NewReaper(queue job.Queue, interval time.Duration, batch int, m *metrics.Metrics, log *slog.Logger) *Reaper {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if batch <= 0 {
		batch = job.DefaultReclaimBatch
	}
	if m == nil {
		m = metrics.New()
	}
	return &Reaper{
		queue:    queue,
		interval: interval,
		batch:    batch,
		metrics:  m,
		log:      log,
	}
}

// Run reclaims expired leases until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) error {
	r.log.Info("lease reaper started", "interval", r.interval, "batch", r.batch)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.log.Info("lease reaper stopped")
			return nil
		case <-ticker.C:
		}

		r.reapOnce(ctx)
	}
}

func (r *Reaper) reapOnce(ctx context.Context) {
	// Keep reaping while a pass comes back full: a full batch means there is
	// probably more waiting, and after a mass outage we want recovery to happen
	// promptly rather than one batch per tick.
	for {
		// Its own timeout so a slow reap cannot stall the loop indefinitely.
		passCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		result, err := r.queue.ReclaimExpired(passCtx, r.batch)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return // shutting down; not an error worth logging
			}
			r.log.Error("reclaiming expired leases failed", "error", err)
			return
		}

		if result.Total() == 0 {
			return
		}

		r.metrics.JobsReclaimed.Add(float64(result.Total()))

		// WARN, not INFO. Reclaiming a lease always means a worker died or
		// stalled while holding work. In a healthy fleet this log line should
		// be rare, and it is exactly the signal you want to alert on.
		r.log.Warn("reclaimed jobs from expired leases",
			"requeued", result.Requeued,
			"dead_lettered", result.DeadLettered,
		)

		if result.Total() < r.batch {
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}
