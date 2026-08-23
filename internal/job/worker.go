package job

import (
	"context"
	"time"
)

// WorkerStatus describes what a worker process is doing.
type WorkerStatus string

const (
	// WorkerActive means the process is claiming and running jobs.
	WorkerActive WorkerStatus = "active"
	// WorkerDraining means it has received a shutdown signal, has stopped
	// claiming, and is finishing what it already holds.
	WorkerDraining WorkerStatus = "draining"
	// WorkerStopped means it exited cleanly and deregistered itself.
	WorkerStopped WorkerStatus = "stopped"
)

func (s WorkerStatus) String() string { return string(s) }

// WorkerInfo is one row of the worker inventory.
type WorkerInfo struct {
	ID            string
	Hostname      string
	PID           int
	Status        WorkerStatus
	Concurrency   int
	ActiveJobs    int
	StartedAt     time.Time
	LastHeartbeat time.Time
	UpdatedAt     time.Time
}

// Alive reports whether the worker has checked in recently enough to be
// believed.
//
// There is no "dead" status in the database, and that is deliberate: a process
// that is killed has no opportunity to write one. Liveness is therefore always
// inferred from the freshness of the last heartbeat, never from a stored flag.
// A stored flag would be a lie exactly when it mattered most.
func (w WorkerInfo) Alive(now time.Time, threshold time.Duration) bool {
	if w.Status == WorkerStopped {
		return false
	}
	return now.Sub(w.LastHeartbeat) <= threshold
}

// WorkerRegistry persists the worker inventory.
//
// A third small interface alongside Repository and Queue, for the same reason:
// only the worker binary registers and heartbeats, and only the API lists. A
// single fat interface would force each side to fake methods it never calls.
type WorkerRegistry interface {
	// Register records a worker as active, overwriting any previous row with
	// the same id. A worker that restarts with the same id must not fail to
	// start because of its own stale row.
	Register(ctx context.Context, w WorkerInfo) error

	// Heartbeat refreshes last_heartbeat and the running-job count.
	Heartbeat(ctx context.Context, id string, activeJobs int) error

	// SetStatus records a lifecycle change, e.g. active -> draining on SIGTERM.
	SetStatus(ctx context.Context, id string, status WorkerStatus) error

	// Deregister marks a worker stopped on clean shutdown.
	Deregister(ctx context.Context, id string) error

	// ListWorkers returns the inventory, most recently seen first.
	ListWorkers(ctx context.Context) ([]*WorkerInfo, error)

	// PruneWorkers deletes rows that have not been seen for longer than
	// olderThan, so the table does not accumulate one row per container that
	// has ever existed.
	PruneWorkers(ctx context.Context, olderThan time.Duration) (int64, error)
}
