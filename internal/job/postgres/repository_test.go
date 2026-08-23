package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
	jobpg "github.com/anjani-kr-singh-ai/taskforge/internal/job/postgres"
)

// These tests talk to a real PostgreSQL. Mocking the driver would prove only
// that our mock behaves like our mock; the behaviour we actually depend on -
// partial unique indexes arbitrating a race, jsonb round-tripping, NULL
// timestamps - lives in the database.
//
// Run them with:
//
//	$env:TASKFORGE_TEST_DSN="postgres://taskforge:taskforge@localhost:5433/taskforge?sslmode=disable"
//	go test ./internal/job/postgres/...
//
// Without the variable they skip, so `go test ./...` stays green on a machine
// with no database.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("TASKFORGE_TEST_DSN")
	if dsn == "" {
		t.Skip("TASKFORGE_TEST_DSN not set; skipping PostgreSQL integration test")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

// uniqueType returns a job type unique to this test run so tests can run
// against a database that already holds data, and clean up only their own rows.
func uniqueType(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test.%s", uuid.NewString()[:8])
}

func cleanup(t *testing.T, pool *pgxpool.Pool, jobType string) {
	t.Helper()
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DELETE FROM jobs WHERE type = $1`, jobType); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
}

// TestCreateAndGetRoundTrip is the type-mapping test: it proves uuid, jsonb,
// NOT NULL timestamptz and NULLable timestamptz/text all survive a write and a
// read intact.
func TestCreateAndGetRoundTrip(t *testing.T) {
	pool := testPool(t)
	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()

	jobType := uniqueType(t)
	cleanup(t, pool, jobType)

	now := time.Now().UTC().Truncate(time.Microsecond) // PostgreSQL stores microseconds
	scheduled := now.Add(2 * time.Hour)
	key := "idem-" + uuid.NewString()

	in := &job.Job{
		ID:             uuid.New(),
		Type:           jobType,
		Payload:        json.RawMessage(`{"to":"user@example.com","subject":"Hello"}`),
		Status:         job.StatusPending,
		Priority:       73,
		MaxRetries:     5,
		ScheduledAt:    &scheduled,
		AvailableAt:    scheduled,
		IdempotencyKey: &key,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	stored, created, err := repo.Create(ctx, in)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !created {
		t.Fatal("Create reported created=false for a fresh idempotency key")
	}

	got, err := repo.GetByID(ctx, in.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}

	if got.ID != in.ID {
		t.Errorf("id = %s, want %s", got.ID, in.ID)
	}
	if got.Status != job.StatusPending {
		t.Errorf("status = %q, want pending", got.Status)
	}
	if got.Priority != 73 {
		t.Errorf("priority = %d, want 73", got.Priority)
	}
	if got.LeaseEpoch != 0 {
		t.Errorf("lease_epoch = %d, want 0", got.LeaseEpoch)
	}

	// NULL columns must come back as nil pointers, not zero values.
	if got.StartedAt != nil {
		t.Errorf("started_at = %v, want nil", got.StartedAt)
	}
	if got.WorkerID != nil {
		t.Errorf("worker_id = %v, want nil", got.WorkerID)
	}
	if got.LastError != nil {
		t.Errorf("last_error = %v, want nil", got.LastError)
	}

	if got.ScheduledAt == nil {
		t.Fatal("scheduled_at = nil, want a timestamp")
	}
	if !got.ScheduledAt.Equal(scheduled) {
		t.Errorf("scheduled_at = %s, want %s", got.ScheduledAt, scheduled)
	}
	if !got.AvailableAt.Equal(scheduled) {
		t.Errorf("available_at = %s, want %s", got.AvailableAt, scheduled)
	}

	// jsonb normalises whitespace and key order, so compare semantically.
	var gotPayload, wantPayload map[string]any
	if err := json.Unmarshal(got.Payload, &gotPayload); err != nil {
		t.Fatalf("payload is not valid JSON: %v (%s)", err, got.Payload)
	}
	if err := json.Unmarshal(in.Payload, &wantPayload); err != nil {
		t.Fatal(err)
	}
	if gotPayload["to"] != wantPayload["to"] || gotPayload["subject"] != wantPayload["subject"] {
		t.Errorf("payload = %s, want %s", got.Payload, in.Payload)
	}

	if stored.ID != got.ID {
		t.Errorf("Create returned id %s but GetByID found %s", stored.ID, got.ID)
	}
}

// TestCreateIsIdempotent proves the deduplication comes from the database
// constraint: the second insert is suppressed and the original job is returned.
func TestCreateIsIdempotent(t *testing.T) {
	pool := testPool(t)
	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()

	jobType := uniqueType(t)
	cleanup(t, pool, jobType)

	key := "idem-" + uuid.NewString()
	now := time.Now().UTC()

	newJob := func() *job.Job {
		return &job.Job{
			ID:             uuid.New(), // deliberately a DIFFERENT id each time
			Type:           jobType,
			Payload:        json.RawMessage(`{}`),
			Status:         job.StatusPending,
			Priority:       job.DefaultPriority,
			MaxRetries:     job.DefaultMaxRetries,
			AvailableAt:    now,
			IdempotencyKey: &key,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
	}

	first, created, err := repo.Create(ctx, newJob())
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	if !created {
		t.Fatal("first Create reported created=false")
	}

	second, created, err := repo.Create(ctx, newJob())
	if err != nil {
		t.Fatalf("second Create: %v", err)
	}
	if created {
		t.Fatal("second Create reported created=true; the idempotency key was not enforced")
	}
	if second.ID != first.ID {
		t.Errorf("second Create returned id %s, want the original %s", second.ID, first.ID)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE idempotency_key = $1`, key).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("found %d rows for the idempotency key, want exactly 1", count)
	}
}

// TestCreateConcurrentIdempotency is the test that a SELECT-then-INSERT
// implementation fails: many goroutines racing on one key must still produce
// exactly one row.
func TestCreateConcurrentIdempotency(t *testing.T) {
	pool := testPool(t)
	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()

	jobType := uniqueType(t)
	cleanup(t, pool, jobType)

	const goroutines = 16
	key := "idem-" + uuid.NewString()
	now := time.Now().UTC()

	type result struct {
		id      uuid.UUID
		created bool
		err     error
	}
	results := make(chan result, goroutines)

	start := make(chan struct{})
	for range goroutines {
		go func() {
			// Every goroutine blocks here, so they all hit the database at once
			// rather than in whatever order they happened to start.
			<-start

			stored, created, err := repo.Create(ctx, &job.Job{
				ID:             uuid.New(),
				Type:           jobType,
				Payload:        json.RawMessage(`{}`),
				Status:         job.StatusPending,
				Priority:       job.DefaultPriority,
				MaxRetries:     job.DefaultMaxRetries,
				AvailableAt:    now,
				IdempotencyKey: &key,
				CreatedAt:      now,
				UpdatedAt:      now,
			})
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{id: stored.ID, created: created}
		}()
	}
	close(start)

	var (
		createdCount int
		winner       uuid.UUID
	)
	for range goroutines {
		r := <-results
		if r.err != nil {
			t.Fatalf("concurrent Create: %v", r.err)
		}
		if r.created {
			createdCount++
			winner = r.id
		}
	}

	if createdCount != 1 {
		t.Errorf("%d goroutines reported created=true, want exactly 1", createdCount)
	}

	var rowCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE idempotency_key = $1`, key).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}
	if rowCount != 1 {
		t.Errorf("database holds %d rows for the key, want exactly 1 (winner %s)", rowCount, winner)
	}
}

func TestGetByIDNotFound(t *testing.T) {
	pool := testPool(t)
	repo := jobpg.NewJobRepository(pool)

	_, err := repo.GetByID(context.Background(), uuid.New())
	if !errors.Is(err, job.ErrNotFound) {
		t.Errorf("error = %v, want job.ErrNotFound", err)
	}
}

func TestListFiltersByTypeAndStatus(t *testing.T) {
	pool := testPool(t)
	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()

	jobType := uniqueType(t)
	cleanup(t, pool, jobType)

	now := time.Now().UTC()
	for i := range 5 {
		_, _, err := repo.Create(ctx, &job.Job{
			ID:          uuid.New(),
			Type:        jobType,
			Payload:     json.RawMessage(`{}`),
			Status:      job.StatusPending,
			Priority:    job.DefaultPriority,
			MaxRetries:  job.DefaultMaxRetries,
			AvailableAt: now.Add(time.Duration(i) * time.Second),
			CreatedAt:   now.Add(time.Duration(i) * time.Second),
			UpdatedAt:   now,
		})
		if err != nil {
			t.Fatalf("seed job %d: %v", i, err)
		}
	}

	got, err := repo.List(ctx, job.ListFilter{
		Type:     jobType,
		Statuses: []job.Status{job.StatusPending},
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d jobs, want 5", len(got))
	}

	// Newest first.
	for i := 1; i < len(got); i++ {
		if got[i-1].CreatedAt.Before(got[i].CreatedAt) {
			t.Errorf("results are not ordered newest-first at index %d", i)
		}
	}

	// A status that no seeded job has must return nothing.
	empty, err := repo.List(ctx, job.ListFilter{
		Type:     jobType,
		Statuses: []job.Status{job.StatusCompleted},
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("List completed: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("got %d completed jobs, want 0", len(empty))
	}
}

func TestCountByStatusReportsEveryStatus(t *testing.T) {
	pool := testPool(t)
	repo := jobpg.NewJobRepository(pool)

	counts, err := repo.CountByStatus(context.Background())
	if err != nil {
		t.Fatalf("CountByStatus: %v", err)
	}

	// Absent statuses must be present as zero, not missing: a gauge that
	// vanishes looks identical to a broken scrape.
	for _, s := range job.Statuses() {
		if _, ok := counts[s]; !ok {
			t.Errorf("status %q missing from the result", s)
		}
	}
}
