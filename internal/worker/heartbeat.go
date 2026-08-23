package worker

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
)

// pruneEvery is how often a heartbeating worker also prunes long-dead rows.
// Rare, because the work is trivial and every worker does it.
const pruneEvery = 20

// Heartbeat keeps this process visible in the worker inventory.
//
// # What it is for, and what it is NOT for
//
// It is for answering "is the fleet healthy?" - how many workers are alive, how
// much capacity exists, is anything stuck. It is explicitly NOT part of job
// claiming: a worker whose heartbeat is failing keeps processing jobs perfectly,
// and jobs are never routed based on this table.
//
// That separation is why every error in here is logged and swallowed. Making a
// worker exit because it could not write an observability row would mean the
// monitoring system taking down the thing it monitors.
//
// Note the asymmetry with leases: a job's lease is authoritative and enforced
// with a fencing token, because getting it wrong duplicates work. A heartbeat is
// advisory, because getting it wrong only produces a misleading dashboard.
type Heartbeat struct {
	registry   job.WorkerRegistry
	info       job.WorkerInfo
	interval   time.Duration
	retention  time.Duration
	activeJobs func() int
	log        *slog.Logger
}

// HeartbeatConfig configures the loop.
type HeartbeatConfig struct {
	WorkerID    string
	Concurrency int
	// Interval is how often to check in. It must be comfortably shorter than
	// the staleness threshold the API uses to judge liveness.
	Interval time.Duration
	// Retention is how long a silent worker's row is kept before pruning.
	Retention time.Duration
	// ActiveJobs reports how many jobs are running right now. Optional.
	ActiveJobs func() int
}

// NewHeartbeat builds the loop.
func NewHeartbeat(registry job.WorkerRegistry, cfg HeartbeatConfig, log *slog.Logger) *Heartbeat {
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.Retention <= 0 {
		cfg.Retention = time.Hour
	}
	if cfg.ActiveJobs == nil {
		cfg.ActiveJobs = func() int { return 0 }
	}

	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}

	return &Heartbeat{
		registry: registry,
		info: job.WorkerInfo{
			ID:          cfg.WorkerID,
			Hostname:    hostname,
			PID:         os.Getpid(),
			Status:      job.WorkerActive,
			Concurrency: cfg.Concurrency,
		},
		interval:   cfg.Interval,
		retention:  cfg.Retention,
		activeJobs: cfg.ActiveJobs,
		log:        log,
	}
}

// Run registers the worker, heartbeats until ctx is cancelled, then
// deregisters.
func (h *Heartbeat) Run(ctx context.Context) error {
	if err := h.registry.Register(ctx, h.info); err != nil {
		// Non-fatal on purpose. A worker that cannot write its inventory row
		// can still do the only job that matters.
		h.log.Error("registering worker failed; continuing without an inventory entry", "error", err)
	} else {
		h.log.Info("worker registered",
			"hostname", h.info.Hostname, "pid", h.info.PID, "concurrency", h.info.Concurrency)
	}

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	ticks := 0
	for {
		select {
		case <-ctx.Done():
			h.shutdown()
			return nil
		case <-ticker.C:
		}

		ticks++
		h.beat(ctx)

		if ticks%pruneEvery == 0 {
			h.prune(ctx)
		}
	}
}

func (h *Heartbeat) beat(ctx context.Context) {
	beatCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	err := h.registry.Heartbeat(beatCtx, h.info.ID, h.activeJobs())
	if err == nil {
		return
	}

	// The row was pruned while we were alive. Re-register rather than
	// heartbeating into the void for the rest of the process's life.
	h.log.Warn("heartbeat failed; re-registering", "error", err)
	if err := h.registry.Register(beatCtx, h.info); err != nil {
		h.log.Error("re-registering worker failed", "error", err)
	}
}

func (h *Heartbeat) prune(ctx context.Context) {
	pruneCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	n, err := h.registry.PruneWorkers(pruneCtx, h.retention)
	if err != nil {
		h.log.Error("pruning dead workers failed", "error", err)
		return
	}
	if n > 0 {
		h.log.Info("pruned dead worker records", "count", n, "older_than", h.retention)
	}
}

// shutdown marks the worker stopped.
func (h *Heartbeat) shutdown() {
	// A fresh context: ctx is already cancelled, and a deregistration written
	// with a cancelled context would never reach the database - leaving the
	// worker looking crashed when it actually shut down cleanly.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), writeTimeout)
	defer cancel()

	if err := h.registry.Deregister(ctx, h.info.ID); err != nil {
		h.log.Error("deregistering worker failed", "error", err)
		return
	}
	h.log.Info("worker deregistered")
}

// Draining marks the worker as shutting down so the fleet view shows it is no
// longer taking new work.
func (h *Heartbeat) Draining(ctx context.Context) {
	if err := h.registry.SetStatus(ctx, h.info.ID, job.WorkerDraining); err != nil {
		h.log.Error("marking worker draining failed", "error", err)
	}
}
