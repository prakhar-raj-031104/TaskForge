package job

import (
	"context"

	"github.com/google/uuid"
)

// Repository is the persistence contract the service depends on.
//
// It is declared HERE, in the package that consumes it, not in the package
// that implements it. That is the idiomatic Go direction and it has a concrete
// payoff: this package has no idea PostgreSQL exists, the service is testable
// with a small in-memory fake, and swapping the storage engine touches one
// sub-package rather than the domain.
//
// It is also deliberately small. A repository interface with thirty methods is
// not an abstraction, it is a second copy of your SQL layer.
type Repository interface {
	// Create inserts a job.
	//
	// When the job carries an idempotency key that already exists, Create must
	// NOT insert a second row. It returns the pre-existing job and created=false
	// so the API can answer 200 rather than 201.
	//
	// The uniqueness must be enforced by the database, not by a prior SELECT:
	// two concurrent requests can both find nothing and both insert.
	Create(ctx context.Context, j *Job) (stored *Job, created bool, err error)

	// GetByID returns a single job, or ErrNotFound.
	GetByID(ctx context.Context, id uuid.UUID) (*Job, error)

	// List returns jobs matching the filter, newest first.
	List(ctx context.Context, f ListFilter) ([]*Job, error)

	// CountByStatus returns the number of jobs in each status. It backs both
	// the queue-depth metric and the readiness endpoint's queue summary.
	CountByStatus(ctx context.Context) (map[Status]int64, error)

	// RetryDeadLetter returns a dead-lettered job to the queue with a fresh
	// retry budget. It returns ErrNotFound when no such job exists, and
	// ErrInvalidTransition when the job is not dead-lettered.
	RetryDeadLetter(ctx context.Context, id uuid.UUID) (*Job, error)
}
