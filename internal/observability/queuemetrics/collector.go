// Package queuemetrics refreshes the gauges that describe queue and fleet
// state.
//
// It lives in its own package rather than inside internal/observability/metrics
// for a concrete reason: it needs the job domain types, and internal/job needs
// the metrics collectors. Putting this code in the metrics package produces
//
//	job -> metrics -> job
//
// which the Go compiler rejects outright. Splitting the domain-aware collector
// out keeps `metrics` a leaf package that knows nothing about jobs, and leaves
// this package as the single place where the two meet.
//
// That is a good illustration of a general point: Go's ban on import cycles is
// not an inconvenience to route around, it is a design check. The cycle here
// was telling us the metrics package had grown a responsibility that did not
// belong to it.
package queuemetrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/metrics"
)

// Stats is what the collector needs from storage.
//
// Declared here, in the consumer, so this collector can be tested with a fake
// and the repository is free to grow methods nobody here cares about.
type Stats interface {
	CountByStatus(ctx context.Context) (map[job.Status]int64, error)
	OldestPendingAge(ctx context.Context) (time.Duration, error)
	ListWorkers(ctx context.Context) ([]*job.WorkerInfo, error)
}

// Collector periodically refreshes queue and fleet gauges.
//
// # Why a background loop and not a Prometheus custom Collector
//
// A custom Collector runs its queries during the scrape. That is tempting, the
// numbers are always fresh, but it puts the database on Prometheus's critical
// path: a slow query makes scrapes time out, and Prometheus records a gap in
// your data at exactly the moment the system is unhealthy and you most need the
// graph.
//
// Refreshing on a timer decouples the two. A scrape only ever reads values
// already in memory and cannot block. The cost is that gauges are up to one
// interval stale, which for queue depth is irrelevant.
type Collector struct {
	stats      Stats
	metrics    *metrics.Metrics
	interval   time.Duration
	staleAfter time.Duration
	log        *slog.Logger
}

// New builds the loop. staleAfter is how long without a heartbeat before a
// worker stops counting towards the live fleet.
func New(stats Stats, m *metrics.Metrics, interval, staleAfter time.Duration, log *slog.Logger) *Collector {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	if staleAfter <= 0 {
		staleAfter = 30 * time.Second
	}
	return &Collector{
		stats:      stats,
		metrics:    m,
		interval:   interval,
		staleAfter: staleAfter,
		log:        log,
	}
}

// Run refreshes the gauges until ctx is cancelled.
func (c *Collector) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Collect immediately so the first scrape after startup has real values
	// rather than zeroes, which would look like an empty queue.
	c.collect(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.collect(ctx)
		}
	}
}

func (c *Collector) collect(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if counts, err := c.stats.CountByStatus(ctx); err != nil {
		c.log.WarnContext(ctx, "collecting queue depth failed", "error", err)
	} else {
		for status, n := range counts {
			// Every status is always set, including zeroes. A gauge that
			// disappears when a queue empties is indistinguishable from a
			// failed scrape, and alerting on absent data pages on-call for a
			// perfectly healthy system.
			c.metrics.QueueDepth.WithLabelValues(status.String()).Set(float64(n))
		}
	}

	if age, err := c.stats.OldestPendingAge(ctx); err != nil {
		c.log.WarnContext(ctx, "collecting oldest pending age failed", "error", err)
	} else {
		// The single best indicator of queue health. Depth alone is ambiguous,
		// since 10,000 jobs draining fast is fine, but a rising oldest-pending
		// age while workers are alive means the fleet cannot keep up.
		c.metrics.OldestPending.Set(age.Seconds())
	}

	if workers, err := c.stats.ListWorkers(ctx); err != nil {
		c.log.WarnContext(ctx, "collecting worker inventory failed", "error", err)
	} else {
		now := time.Now()
		var alive, capacity int
		for _, w := range workers {
			if w.Alive(now, c.staleAfter) {
				alive++
				capacity += w.Concurrency
			}
		}
		c.metrics.ActiveWorkers.Set(float64(alive))
		c.metrics.FleetCapacity.Set(float64(capacity))
	}
}
