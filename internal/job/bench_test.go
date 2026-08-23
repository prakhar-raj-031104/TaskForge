package job

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Pure-Go benchmarks. No database, so these run anywhere:
//
//	go test ./internal/job/ -bench=. -benchmem -run=^$

// BenchmarkBackoffDelay measures the retry-delay computation, which runs once
// per failed job. It uses math.Pow and a random draw, so it is worth knowing it
// is nowhere near the cost of the database write that follows it.
func BenchmarkBackoffDelay(b *testing.B) {
	backoff := DefaultBackoff

	b.ReportAllocs()
	for b.Loop() {
		_ = backoff.Delay(5)
	}
}

// BenchmarkRegistryGet matters because it happens on every single job, inside
// the worker's hot path, under an RWMutex shared by every execution goroutine.
func BenchmarkRegistryGet(b *testing.B) {
	r := NewRegistry()
	for i := range 16 {
		if err := r.Register(fmt.Sprintf("type_%d", i),
			HandlerFunc(func(context.Context, *Job) error { return nil })); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := r.Get("type_8"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRegistryGetParallel is the realistic shape: many worker goroutines
// reading concurrently. An RWMutex allows unlimited concurrent readers, so this
// should scale; a plain Mutex would serialise every job dispatch in the process.
func BenchmarkRegistryGetParallel(b *testing.B) {
	r := NewRegistry()
	for i := range 16 {
		if err := r.Register(fmt.Sprintf("type_%d", i),
			HandlerFunc(func(context.Context, *Job) error { return nil })); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := r.Get("type_8"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkCreateParamsValidate measures request validation, which runs on
// every POST /api/v1/jobs before any I/O happens.
func BenchmarkCreateParamsValidate(b *testing.B) {
	now := time.Now()
	scheduled := now.Add(time.Hour)
	key := "order-123-email"
	priority := 90

	params := CreateParams{
		Type:           "send_email",
		Payload:        json.RawMessage(`{"to":"user@example.com","subject":"Hello"}`),
		Priority:       &priority,
		ScheduledAt:    &scheduled,
		IdempotencyKey: &key,
	}

	b.ReportAllocs()
	for b.Loop() {
		if err := params.Validate(now); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStateTransition measures a state-machine check.
func BenchmarkStateTransition(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = StatusRunning.CanTransitionTo(StatusCompleted)
	}
}

// BenchmarkServiceCreate measures the full create use case against an
// in-memory repository, isolating the Go-side cost from the database round
// trip that dominates BenchmarkCreate in the postgres package.
func BenchmarkServiceCreate(b *testing.B) {
	repo := newBenchRepo()
	svc := NewService(repo, discardLogger())

	params := CreateParams{
		Type:    "send_email",
		Payload: json.RawMessage(`{"to":"user@example.com"}`),
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := svc.Create(context.Background(), params); err != nil {
			b.Fatal(err)
		}
	}
}

// benchRepo is a Repository that does nothing, so the benchmark measures the
// service and not a map.
type benchRepo struct{}

func newBenchRepo() *benchRepo { return &benchRepo{} }

func (r *benchRepo) Create(_ context.Context, j *Job) (*Job, bool, error) { return j, true, nil }
func (r *benchRepo) GetByID(context.Context, uuid.UUID) (*Job, error)     { return nil, ErrNotFound }
func (r *benchRepo) List(context.Context, ListFilter) ([]*Job, error)     { return nil, nil }
func (r *benchRepo) RetryDeadLetter(context.Context, uuid.UUID) (*Job, error) {
	return nil, ErrNotFound
}
func (r *benchRepo) CountByStatus(context.Context) (map[Status]int64, error) { return nil, nil }
