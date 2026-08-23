package job

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
)

// fakeRepo is an in-memory Repository.
//
// This is the payoff from declaring Repository in the consuming package: the
// service's whole test suite runs with no database, no container and no
// network, in microseconds. The PostgreSQL behaviour it stands in for is tested
// separately, against real PostgreSQL, in job/postgres.
type fakeRepo struct {
	jobs      map[uuid.UUID]*Job
	byKey     map[string]*Job
	createErr error
	calls     int
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		jobs:  make(map[uuid.UUID]*Job),
		byKey: make(map[string]*Job),
	}
}

func (r *fakeRepo) Create(_ context.Context, j *Job) (*Job, bool, error) {
	r.calls++
	if r.createErr != nil {
		return nil, false, r.createErr
	}
	if j.IdempotencyKey != nil {
		if existing, ok := r.byKey[*j.IdempotencyKey]; ok {
			return existing, false, nil
		}
		r.byKey[*j.IdempotencyKey] = j
	}
	r.jobs[j.ID] = j
	return j, true, nil
}

func (r *fakeRepo) GetByID(_ context.Context, id uuid.UUID) (*Job, error) {
	j, ok := r.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return j, nil
}

func (r *fakeRepo) List(_ context.Context, f ListFilter) ([]*Job, error) {
	out := make([]*Job, 0, len(r.jobs))
	for _, j := range r.jobs {
		if f.Type != "" && j.Type != f.Type {
			continue
		}
		out = append(out, j)
	}
	return out, nil
}

func (r *fakeRepo) RetryDeadLetter(_ context.Context, id uuid.UUID) (*Job, error) {
	j, ok := r.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	if j.Status != StatusDeadLetter {
		return nil, &InvalidTransitionError{From: j.Status, To: StatusPending}
	}
	j.Status = StatusPending
	j.Attempts = 0
	return j, nil
}

func (r *fakeRepo) CountByStatus(context.Context) (map[Status]int64, error) {
	counts := make(map[Status]int64)
	for _, s := range Statuses() {
		counts[s] = 0
	}
	for _, j := range r.jobs {
		counts[j.Status]++
	}
	return counts, nil
}

// discardLogger keeps test output readable.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestService(t *testing.T, repo Repository) *Service {
	t.Helper()
	return NewService(repo, discardLogger(), WithClock(func() time.Time { return fixedNow }))
}

func TestServiceCreateAppliesDefaults(t *testing.T) {
	t.Parallel()

	repo := newFakeRepo()
	svc := newTestService(t, repo)

	got, created, err := svc.Create(context.Background(), CreateParams{Type: "send_email"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !created {
		t.Error("created = false, want true for a fresh job")
	}

	if got.Status != StatusPending {
		t.Errorf("status = %q, want pending", got.Status)
	}
	if got.Priority != DefaultPriority {
		t.Errorf("priority = %d, want the default %d", got.Priority, DefaultPriority)
	}
	if got.MaxRetries != DefaultMaxRetries {
		t.Errorf("max_retries = %d, want the default %d", got.MaxRetries, DefaultMaxRetries)
	}
	if got.Attempts != 0 {
		t.Errorf("attempts = %d, want 0", got.Attempts)
	}
	if got.ID == uuid.Nil {
		t.Error("id was not generated")
	}
	// An absent payload becomes an empty object so no handler has to nil-check.
	if string(got.Payload) != `{}` {
		t.Errorf("payload = %s, want {}", got.Payload)
	}
	// No schedule requested: available immediately, and scheduled_at stays NULL.
	if got.ScheduledAt != nil {
		t.Errorf("scheduled_at = %v, want nil", got.ScheduledAt)
	}
	if !got.AvailableAt.Equal(fixedNow) {
		t.Errorf("available_at = %s, want %s", got.AvailableAt, fixedNow)
	}
}

// TestServiceCreateSchedulingSeedsAvailableAt is the scheduled_at /
// available_at split from the design doc, asserted.
func TestServiceCreateSchedulingSeedsAvailableAt(t *testing.T) {
	t.Parallel()

	repo := newFakeRepo()
	svc := newTestService(t, repo)

	when := fixedNow.Add(3 * time.Hour)
	got, _, err := svc.Create(context.Background(), CreateParams{
		Type:        "send_email",
		ScheduledAt: &when,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if got.ScheduledAt == nil || !got.ScheduledAt.Equal(when) {
		t.Errorf("scheduled_at = %v, want %s", got.ScheduledAt, when)
	}
	if !got.AvailableAt.Equal(when) {
		t.Errorf("available_at = %s, want it seeded from scheduled_at (%s)", got.AvailableAt, when)
	}
}

func TestServiceCreateStoresTimesInUTC(t *testing.T) {
	t.Parallel()

	kolkata, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Skipf("timezone database unavailable: %v", err)
	}

	repo := newFakeRepo()
	svc := newTestService(t, repo)

	when := fixedNow.Add(time.Hour).In(kolkata)
	got, _, err := svc.Create(context.Background(), CreateParams{
		Type:        "send_email",
		ScheduledAt: &when,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if got.ScheduledAt.Location() != time.UTC {
		t.Errorf("scheduled_at location = %s, want UTC", got.ScheduledAt.Location())
	}
	// Normalising the zone must not move the instant.
	if !got.ScheduledAt.Equal(when) {
		t.Errorf("scheduled_at = %s, which is a different instant from %s", got.ScheduledAt, when)
	}
}

func TestServiceCreateRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	repo := newFakeRepo()
	svc := newTestService(t, repo)

	_, _, err := svc.Create(context.Background(), CreateParams{Type: "NOT VALID"})

	var v *ValidationError
	if !errors.As(err, &v) {
		t.Fatalf("error = %v, want a *ValidationError", err)
	}
	if repo.calls != 0 {
		t.Errorf("repository was called %d times; validation must run before any write", repo.calls)
	}
}

func TestServiceCreateIsIdempotent(t *testing.T) {
	t.Parallel()

	repo := newFakeRepo()
	svc := newTestService(t, repo)

	params := CreateParams{Type: "send_email", IdempotencyKey: ptr("order-9")}

	first, created, err := svc.Create(context.Background(), params)
	if err != nil || !created {
		t.Fatalf("first Create: job=%v created=%v err=%v", first, created, err)
	}

	second, created, err := svc.Create(context.Background(), params)
	if err != nil {
		t.Fatalf("second Create: %v", err)
	}
	if created {
		t.Error("created = true on the second call, want false")
	}
	if second.ID != first.ID {
		t.Errorf("second call returned id %s, want the original %s", second.ID, first.ID)
	}
}

func TestServiceCreateWrapsRepositoryErrors(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("connection reset")
	repo := newFakeRepo()
	repo.createErr = sentinel
	svc := newTestService(t, repo)

	_, _, err := svc.Create(context.Background(), CreateParams{Type: "send_email"})

	// Wrapped with %w, so the original is still discoverable by callers that
	// care, while the message gains context for callers that do not.
	if !errors.Is(err, sentinel) {
		t.Errorf("errors.Is could not find the underlying error in %v", err)
	}
}

func TestServiceGetPropagatesNotFound(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, newFakeRepo())

	_, err := svc.Get(context.Background(), uuid.New())
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestServiceListValidatesBeforeQuerying(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, newFakeRepo())

	_, err := svc.List(context.Background(), ListFilter{Limit: MaxListLimit + 1})

	var v *ValidationError
	if !errors.As(err, &v) {
		t.Fatalf("error = %v, want a *ValidationError", err)
	}
}
