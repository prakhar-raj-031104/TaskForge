package job

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/metrics"
)

// Service holds the job use cases. It depends on the Repository interface, not
// on PostgreSQL, so every test in this package runs without a database.
type Service struct {
	repo    Repository
	log     *slog.Logger
	now     func() time.Time
	metrics *metrics.Metrics
}

// ServiceOption customises a Service. Functional options keep the common
// constructor call short while leaving room for rarely-used knobs, without the
// combinatorial explosion of NewServiceWithClockAndLogger constructors.
type ServiceOption func(*Service)

// WithClock replaces the time source. Tests use it to make scheduling
// deterministic; production never calls it.
func WithClock(now func() time.Time) ServiceOption {
	return func(s *Service) { s.now = now }
}

// WithMetrics attaches a metrics registry. Without it the service records into
// a private one that nobody scrapes, so instrumentation code needs no nil
// checks.
func WithMetrics(m *metrics.Metrics) ServiceOption {
	return func(s *Service) {
		if m != nil {
			s.metrics = m
		}
	}
}

// NewService builds a Service. Dependencies are passed in explicitly rather
// than reached for through globals, which is what makes the zero-database test
// above possible.
func NewService(repo Repository, log *slog.Logger, opts ...ServiceOption) *Service {
	s := &Service{
		repo:    repo,
		log:     log,
		now:     time.Now,
		metrics: metrics.New(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Create validates a request and stores the job.
//
// created is false when an idempotency key matched an existing job, in which
// case that job is returned untouched.
func (s *Service) Create(ctx context.Context, p CreateParams) (stored *Job, created bool, err error) {
	now := s.now().UTC()

	if err := p.Validate(now); err != nil {
		return nil, false, err
	}

	j := &Job{
		// The id is minted here, in the application, rather than by the
		// database. That means we can log it before the INSERT and hand it back
		// to the caller even if the write has to be retried — with a
		// database-generated id, a retried INSERT would produce a second row
		// with a different id and no way to correlate the two.
		ID:         uuid.New(),
		Type:       p.Type,
		Payload:    normalizePayload(p.Payload),
		Status:     StatusPending,
		Priority:   valueOr(p.Priority, DefaultPriority),
		Attempts:   0,
		MaxRetries: valueOr(p.MaxRetries, DefaultMaxRetries),
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if p.ScheduledAt != nil {
		scheduled := p.ScheduledAt.UTC()
		j.ScheduledAt = &scheduled
		// available_at starts as a copy of the user's intent and is the only
		// one of the two that retries are allowed to move.
		j.AvailableAt = scheduled
	} else {
		j.AvailableAt = now
	}

	if p.IdempotencyKey != nil {
		key := *p.IdempotencyKey
		j.IdempotencyKey = &key
	}

	stored, created, err = s.repo.Create(ctx, j)
	if err != nil {
		return nil, false, fmt.Errorf("create job: %w", err)
	}

	if !created {
		s.log.InfoContext(ctx, "job creation deduplicated by idempotency key",
			"job_id", stored.ID,
			"job_type", stored.Type,
		)
		return stored, false, nil
	}

	// Counted only when a row was genuinely inserted, so an idempotent retry
	// does not inflate the create rate and make traffic look higher than it is.
	s.metrics.JobsCreated.WithLabelValues(stored.Type).Inc()

	s.log.InfoContext(ctx, "job created",
		"job_id", stored.ID,
		"job_type", stored.Type,
		"priority", stored.Priority,
		"available_at", stored.AvailableAt,
		// The payload is deliberately absent: it routinely holds email
		// addresses, tokens and customer data, and logs are the least
		// access-controlled surface in most systems.
	)

	return stored, true, nil
}

// Get returns a single job by id.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (*Job, error) {
	j, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return j, nil
}

// List returns jobs matching the filter.
func (s *Service) List(ctx context.Context, f ListFilter) ([]*Job, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	jobs, err := s.repo.List(ctx, f.Normalize())
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return jobs, nil
}

// ListDeadLetter returns dead-lettered jobs, newest first.
//
// # Why a dead-letter queue exists at all
//
// Without one, a job that has exhausted its retries has two possible fates,
// both bad: deleted, in which case the work is silently lost and nobody finds
// out until a customer complains; or left pending forever, in which case it is
// retried until the end of time, burning capacity and hammering whatever
// dependency is already broken.
//
// The DLQ is the third option: stop retrying, keep the job, keep the error, and
// make it visible. It turns "some jobs are failing" into an inspectable list
// with a count you can alert on, and every job in it is replayable once the
// cause is fixed.
func (s *Service) ListDeadLetter(ctx context.Context, f ListFilter) ([]*Job, error) {
	// The status filter is forced rather than accepted from the caller: this
	// endpoint means dead-letter, and letting a query parameter widen it would
	// be a surprising way to dump the whole table.
	f.Statuses = []Status{StatusDeadLetter}

	if err := f.Validate(); err != nil {
		return nil, err
	}
	jobs, err := s.repo.List(ctx, f.Normalize())
	if err != nil {
		return nil, fmt.Errorf("list dead-letter jobs: %w", err)
	}
	return jobs, nil
}

// RetryDeadLetter puts a dead-lettered job back on the queue with a fresh
// retry budget.
func (s *Service) RetryDeadLetter(ctx context.Context, id uuid.UUID) (*Job, error) {
	j, err := s.repo.RetryDeadLetter(ctx, id)
	if err != nil {
		return nil, err
	}

	s.log.InfoContext(ctx, "dead-lettered job replayed",
		"job_id", j.ID,
		"job_type", j.Type,
	)
	return j, nil
}

// CountByStatus returns queue depth per status.
func (s *Service) CountByStatus(ctx context.Context) (map[Status]int64, error) {
	counts, err := s.repo.CountByStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("count jobs by status: %w", err)
	}
	return counts, nil
}

// normalizePayload turns an absent payload into an empty JSON object so that
// neither the database nor a handler has to special-case NULL.
func normalizePayload(p json.RawMessage) json.RawMessage {
	if len(p) == 0 {
		return json.RawMessage(`{}`)
	}
	return p
}

// valueOr dereferences p, falling back to def when it is nil.
//
// A generic helper is worth it here only because the alternative is the same
// four lines written twice with different types; [T any] keeps it honest
// without inventing an abstraction.
func valueOr[T any](p *T, def T) T {
	if p == nil {
		return def
	}
	return *p
}
