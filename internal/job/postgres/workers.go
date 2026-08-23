package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
)

var _ job.WorkerRegistry = (*JobRepository)(nil)

const workerColumns = `
	id, hostname, pid, status, concurrency, active_jobs,
	started_at, last_heartbeat, updated_at`

// Register records a worker as active.
//
// ON CONFLICT DO UPDATE rather than INSERT: a worker restarting with a stable
// WORKER_ID (a StatefulSet pod, say) would otherwise collide with its own stale
// row and fail to start. Recovering from a crash must never be blocked by the
// evidence of that crash.
func (r *JobRepository) Register(ctx context.Context, w job.WorkerInfo) error {
	const query = `
		INSERT INTO workers (id, hostname, pid, status, concurrency, active_jobs,
		                     started_at, last_heartbeat)
		VALUES ($1, $2, $3, $4, $5, 0, now(), now())
		ON CONFLICT (id) DO UPDATE
		   SET hostname       = EXCLUDED.hostname,
		       pid            = EXCLUDED.pid,
		       status         = EXCLUDED.status,
		       concurrency    = EXCLUDED.concurrency,
		       active_jobs    = 0,
		       started_at     = now(),
		       last_heartbeat = now()`

	_, err := r.db.Exec(ctx, query, w.ID, w.Hostname, w.PID, string(job.WorkerActive), w.Concurrency)
	if err != nil {
		return fmt.Errorf("register worker %s: %w", w.ID, err)
	}
	return nil
}

// Heartbeat refreshes the worker's liveness timestamp.
func (r *JobRepository) Heartbeat(ctx context.Context, id string, activeJobs int) error {
	const query = `
		UPDATE workers
		   SET last_heartbeat = now(),
		       active_jobs    = $2
		 WHERE id = $1`

	tag, err := r.db.Exec(ctx, query, id, activeJobs)
	if err != nil {
		return fmt.Errorf("heartbeat for worker %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		// The row was pruned while this worker was alive, which is unusual but
		// harmless. Report it so the caller can re-register rather than
		// heartbeating into the void forever.
		return fmt.Errorf("heartbeat for worker %s: %w", id, job.ErrNotFound)
	}
	return nil
}

// SetStatus records a lifecycle transition.
func (r *JobRepository) SetStatus(ctx context.Context, id string, status job.WorkerStatus) error {
	const query = `UPDATE workers SET status = $2, last_heartbeat = now() WHERE id = $1`

	if _, err := r.db.Exec(ctx, query, id, string(status)); err != nil {
		return fmt.Errorf("set worker %s status: %w", id, err)
	}
	return nil
}

// Deregister marks a worker stopped.
//
// The row is updated rather than deleted so that "this worker shut down
// cleanly" stays distinguishable from "this worker vanished" for as long as the
// row survives. Pruning removes it later.
func (r *JobRepository) Deregister(ctx context.Context, id string) error {
	return r.SetStatus(ctx, id, job.WorkerStopped)
}

// ListWorkers returns the inventory, most recently seen first.
func (r *JobRepository) ListWorkers(ctx context.Context) ([]*job.WorkerInfo, error) {
	query := `SELECT ` + workerColumns + ` FROM workers ORDER BY last_heartbeat DESC`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	defer rows.Close()

	var workers []*job.WorkerInfo
	for rows.Next() {
		var (
			w      job.WorkerInfo
			status string
		)
		if err := rows.Scan(&w.ID, &w.Hostname, &w.PID, &status, &w.Concurrency,
			&w.ActiveJobs, &w.StartedAt, &w.LastHeartbeat, &w.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan worker: %w", err)
		}
		w.Status = job.WorkerStatus(status)
		workers = append(workers, &w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workers: %w", err)
	}

	return workers, nil
}

// PruneWorkers deletes long-dead worker rows.
//
// Without this the table grows one row per container that has ever run, which
// in an autoscaled deployment is thousands of rows of pure noise obscuring the
// handful that are actually alive.
func (r *JobRepository) PruneWorkers(ctx context.Context, olderThan time.Duration) (int64, error) {
	const query = `
		DELETE FROM workers
		 WHERE last_heartbeat < now() - make_interval(secs => $1)`

	tag, err := r.db.Exec(ctx, query, olderThan.Seconds())
	if err != nil {
		return 0, fmt.Errorf("prune workers: %w", err)
	}
	return tag.RowsAffected(), nil
}
