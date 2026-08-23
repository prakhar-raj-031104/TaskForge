// Package postgres is the pgx-backed implementation of job.Repository.
//
// It is the only package in the project that imports pgx. Everything above it
// depends on the job.Repository interface, which is why the domain and the
// service have no idea PostgreSQL exists.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
)

// DB is the slice of *pgxpool.Pool this repository needs.
//
// Accepting an interface rather than *pgxpool.Pool means the same repository
// works against a pool or against a pgx.Tx, which is what lets a worker run
// "claim the job" and "record the result" inside one transaction later on.
type DB interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// JobRepository stores jobs in PostgreSQL.
type JobRepository struct {
	db DB
}

// NewJobRepository builds a repository over db.
func NewJobRepository(db DB) *JobRepository {
	return &JobRepository{db: db}
}

// Compile-time proof that this type satisfies the domain contract. If the
// interface gains a method, the build breaks here with a clear message rather
// than at some distant call site.
var _ job.Repository = (*JobRepository)(nil)

// jobColumns is the single source of truth for SELECT ordering. Every scan
// helper reads columns in exactly this order, so adding a column is a two-line
// change instead of a hunt through the file.
const jobColumns = `
	id, type, payload, status, priority, attempts, max_retries,
	scheduled_at, available_at, started_at, completed_at, failed_at,
	lease_until, lease_epoch, worker_id, idempotency_key, last_error,
	created_at, updated_at`

// Create inserts a job, deduplicating on the idempotency key.
//
// The ON CONFLICT clause - not a preceding SELECT - is what makes this safe.
// Two concurrent requests carrying the same key would both find nothing with a
// SELECT and both insert; here the partial unique index arbitrates, exactly one
// INSERT wins, and the loser is told so by getting zero rows back.
func (r *JobRepository) Create(ctx context.Context, j *job.Job) (*job.Job, bool, error) {
	const query = `
		INSERT INTO jobs (
			id, type, payload, status, priority, attempts, max_retries,
			scheduled_at, available_at, idempotency_key, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
		DO NOTHING
		RETURNING ` + jobColumns

	row := r.db.QueryRow(ctx, query,
		j.ID,
		j.Type,
		[]byte(j.Payload),
		string(j.Status),
		j.Priority,
		j.Attempts,
		j.MaxRetries,
		j.ScheduledAt,
		j.AvailableAt,
		j.IdempotencyKey,
		j.CreatedAt,
		j.UpdatedAt,
	)

	stored, err := scanJob(row)
	switch {
	case err == nil:
		return stored, true, nil

	case errors.Is(err, pgx.ErrNoRows):
		// DO NOTHING suppressed the insert, so a row with this key already
		// exists. RETURNING yields nothing in that case, which is precisely how
		// we detect the duplicate.
		if j.IdempotencyKey == nil {
			return nil, false, errors.New("insert affected no rows and no idempotency key was set")
		}
		existing, err := r.getByIdempotencyKey(ctx, *j.IdempotencyKey)
		if err != nil {
			return nil, false, fmt.Errorf("fetch job that won the idempotency race: %w", err)
		}
		return existing, false, nil

	default:
		return nil, false, fmt.Errorf("insert job: %w", err)
	}
}

// GetByID returns one job or job.ErrNotFound.
func (r *JobRepository) GetByID(ctx context.Context, id uuid.UUID) (*job.Job, error) {
	query := `SELECT ` + jobColumns + ` FROM jobs WHERE id = $1`

	j, err := scanJob(r.db.QueryRow(ctx, query, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, job.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job %s: %w", id, err)
	}
	return j, nil
}

func (r *JobRepository) getByIdempotencyKey(ctx context.Context, key string) (*job.Job, error) {
	query := `SELECT ` + jobColumns + ` FROM jobs WHERE idempotency_key = $1`

	j, err := scanJob(r.db.QueryRow(ctx, query, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, job.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job by idempotency key: %w", err)
	}
	return j, nil
}

// List returns jobs matching the filter, newest first.
func (r *JobRepository) List(ctx context.Context, f job.ListFilter) ([]*job.Job, error) {
	var (
		conditions []string
		args       []any
	)

	// Only ever interpolate PLACEHOLDERS into the SQL string. Values travel
	// separately over the wire as bound parameters, so a job type of
	// "'; DROP TABLE jobs; --" is data, not syntax. This is the difference
	// between building SQL and building SQL injection.
	if len(f.Statuses) > 0 {
		statuses := make([]string, len(f.Statuses))
		for i, s := range f.Statuses {
			statuses[i] = string(s)
		}
		args = append(args, statuses)
		conditions = append(conditions, fmt.Sprintf("status = ANY($%d)", len(args)))
	}
	if f.Type != "" {
		args = append(args, f.Type)
		conditions = append(conditions, fmt.Sprintf("type = $%d", len(args)))
	}

	var sb strings.Builder
	sb.WriteString(`SELECT ` + jobColumns + ` FROM jobs`)
	if len(conditions) > 0 {
		sb.WriteString(" WHERE " + strings.Join(conditions, " AND "))
	}
	args = append(args, f.Limit)
	sb.WriteString(fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args)))
	args = append(args, f.Offset)
	sb.WriteString(fmt.Sprintf(" OFFSET $%d", len(args)))

	rows, err := r.db.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	// pgx.Rows holds a connection until it is closed or fully drained. Missing
	// this defer leaks a connection per call and exhausts the pool under load.
	defer rows.Close()

	jobs := make([]*job.Job, 0, f.Limit)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, j)
	}
	// rows.Err reports failures that happened mid-iteration, which the loop
	// above cannot see: without this check a truncated result set looks like a
	// short page.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate jobs: %w", err)
	}

	return jobs, nil
}

// CountByStatus returns the number of jobs in each status.
func (r *JobRepository) CountByStatus(ctx context.Context) (map[job.Status]int64, error) {
	const query = `SELECT status, count(*) FROM jobs GROUP BY status`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("count jobs by status: %w", err)
	}
	defer rows.Close()

	counts := make(map[job.Status]int64, len(job.Statuses()))
	// Report a zero for every status rather than omitting it. A Prometheus
	// gauge that disappears when a queue empties is indistinguishable from a
	// scrape failure, and alerts on "no data" are how on-call gets paged at 3am
	// for a healthy system.
	for _, s := range job.Statuses() {
		counts[s] = 0
	}

	for rows.Next() {
		var (
			status string
			count  int64
		)
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("scan status count: %w", err)
		}
		counts[job.Status(status)] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate status counts: %w", err)
	}

	return counts, nil
}

// OldestPendingAge returns how long the oldest claimable job has been waiting.
//
// This is the single most useful queue-health number. Depth alone is ambiguous:
// 10,000 jobs draining in seconds is fine, while 50 jobs that have been waiting
// an hour is an outage. Age answers "is work actually moving".
//
// The query is served by jobs_claim_idx: available_at is the second index
// column, so PostgreSQL finds the minimum without scanning the table.
func (r *JobRepository) OldestPendingAge(ctx context.Context) (time.Duration, error) {
	const query = `
		SELECT coalesce(extract(epoch FROM (now() - min(available_at))), 0)
		  FROM jobs
		 WHERE status = 'pending'
		   AND available_at <= now()`

	var seconds float64
	if err := r.db.QueryRow(ctx, query).Scan(&seconds); err != nil {
		return 0, fmt.Errorf("oldest pending age: %w", err)
	}
	if seconds < 0 {
		seconds = 0
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

// scanner is satisfied by both pgx.Row (single result) and pgx.Rows (during
// iteration), so one scan helper serves every query in this file.
type scanner interface {
	Scan(dest ...any) error
}

func scanJob(s scanner) (*job.Job, error) {
	var (
		j       job.Job
		payload []byte
		status  string
	)

	err := s.Scan(
		&j.ID, &j.Type, &payload, &status, &j.Priority, &j.Attempts, &j.MaxRetries,
		&j.ScheduledAt, &j.AvailableAt, &j.StartedAt, &j.CompletedAt, &j.FailedAt,
		&j.LeaseUntil, &j.LeaseEpoch, &j.WorkerID, &j.IdempotencyKey, &j.LastError,
		&j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	j.Status = job.Status(status)
	j.Payload = json.RawMessage(payload)

	return &j, nil
}
