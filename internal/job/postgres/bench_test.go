package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
	jobpg "github.com/anjani-kr-singh-ai/taskforge/internal/job/postgres"
)

// Benchmarks run against a real PostgreSQL:
//
//	$env:TASKFORGE_TEST_DSN="postgres://taskforge:taskforge@localhost:5433/taskforge?sslmode=disable"
//	go test ./internal/job/postgres/ -bench=. -benchmem -run=^$ -benchtime=2s
//
// -run=^$ matters: without it every test runs before the benchmarks and the
// timings include their setup.
//
// These numbers describe ONE machine with a containerised database on the same
// host, so they are useful for comparing changes to this code, not for
// predicting production capacity. Never quote a benchmark you have not run.

func benchPool(b *testing.B) *pgxpool.Pool {
	b.Helper()

	dsn := os.Getenv("TASKFORGE_TEST_DSN")
	if dsn == "" {
		b.Skip("TASKFORGE_TEST_DSN not set; skipping database benchmark")
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		b.Fatalf("parse dsn: %v", err)
	}
	// Enough connections that RunParallel is not simply measuring pool
	// contention instead of the query.
	cfg.MaxConns = 32

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		b.Fatalf("connect: %v", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		b.Fatalf("ping: %v", err)
	}
	b.Cleanup(pool.Close)

	return pool
}

func benchType(b *testing.B) string {
	b.Helper()
	t := fmt.Sprintf("bench.%s", uuid.NewString()[:8])
	b.Cleanup(func() {
		if _, err := benchExec(b, `DELETE FROM jobs WHERE type = $1`, t); err != nil {
			b.Logf("cleanup: %v", err)
		}
	})
	return t
}

// benchExec is set by seedBench so cleanup can reach the pool.
var benchExec = func(b *testing.B, sql string, args ...any) (int64, error) {
	return 0, nil
}

// seedBench bulk-inserts n claimable jobs in a single statement.
//
// Row-by-row insertion of 50,000 jobs would take longer than the benchmark it
// is preparing for. generate_series pushes the whole thing into one round trip.
func seedBench(b *testing.B, pool *pgxpool.Pool, jobType string, n int) {
	b.Helper()

	benchExec = func(b *testing.B, sql string, args ...any) (int64, error) {
		tag, err := pool.Exec(context.Background(), sql, args...)
		return tag.RowsAffected(), err
	}

	_, err := pool.Exec(context.Background(), `
		INSERT INTO jobs (id, type, payload, status, priority, max_retries, available_at, created_at, updated_at)
		SELECT gen_random_uuid(), $1, jsonb_build_object('seq', i), 'pending',
		       1 + (i % 100), 3, now() - make_interval(secs => i), now(), now()
		  FROM generate_series(1, $2) AS i`, jobType, n)
	if err != nil {
		b.Fatalf("seed %d jobs: %v", n, err)
	}
}

// resetBench returns every job of this type to pending without timing the work.
func resetBench(b *testing.B, pool *pgxpool.Pool, jobType string) {
	b.Helper()
	if _, err := pool.Exec(context.Background(), `
		UPDATE jobs
		   SET status = 'pending', lease_until = NULL, worker_id = NULL,
		       attempts = 0, started_at = NULL, available_at = now() - interval '1 second'
		 WHERE type = $1`, jobType); err != nil {
		b.Fatalf("reset: %v", err)
	}
}

// BenchmarkCreate measures a single job insert, including the ON CONFLICT
// machinery and the RETURNING round trip.
func BenchmarkCreate(b *testing.B) {
	pool := benchPool(b)
	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()

	jobType := benchType(b)
	benchExec = func(b *testing.B, sql string, args ...any) (int64, error) {
		tag, err := pool.Exec(context.Background(), sql, args...)
		return tag.RowsAffected(), err
	}

	payload := json.RawMessage(`{"to":"user@example.com","subject":"Hello"}`)
	now := time.Now().UTC()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if _, _, err := repo.Create(ctx, &job.Job{
			ID:          uuid.New(),
			Type:        jobType,
			Payload:     payload,
			Status:      job.StatusPending,
			Priority:    job.DefaultPriority,
			MaxRetries:  job.DefaultMaxRetries,
			AvailableAt: now,
			CreatedAt:   now,
			UpdatedAt:   now,
		}); err != nil {
			b.Fatalf("Create: %v", err)
		}
	}
}

// BenchmarkClaim measures the claim query at several batch sizes.
//
// Batch size is the interesting variable: claiming 20 jobs in one round trip
// against a queue of 50,000 should cost far less than 20x a single claim,
// because the fixed costs - parse, plan, lock acquisition, network round trip -
// are paid once.
func BenchmarkClaim(b *testing.B) {
	for _, batch := range []int{1, 5, 20, 50} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			pool := benchPool(b)
			repo := jobpg.NewJobRepository(pool)
			ctx := context.Background()

			jobType := benchType(b)
			seedBench(b, pool, jobType, 50_000)

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				got, err := repo.Claim(ctx, job.ClaimParams{
					WorkerID:      "bench",
					Limit:         batch,
					LeaseDuration: time.Minute,
				})
				if err != nil {
					b.Fatalf("Claim: %v", err)
				}
				if len(got) == 0 {
					// The pool ran dry. Refill it with the timer stopped so the
					// reset never counts towards the measurement.
					b.StopTimer()
					resetBench(b, pool, jobType)
					b.StartTimer()
				}
			}
		})
	}
}

// BenchmarkClaimParallel is the number that matters for a fleet: many workers
// claiming simultaneously.
//
// If SKIP LOCKED were removed, this benchmark would collapse - workers would
// serialise behind each other's row locks and adding parallelism would make
// throughput worse, not better.
func BenchmarkClaimParallel(b *testing.B) {
	pool := benchPool(b)
	ctx := context.Background()

	jobType := benchType(b)
	seedBench(b, pool, jobType, 200_000)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine acts as an independent worker with its own identity.
		repo := jobpg.NewJobRepository(pool)
		workerID := uuid.NewString()

		for pb.Next() {
			if _, err := repo.Claim(ctx, job.ClaimParams{
				WorkerID:      workerID,
				Limit:         10,
				LeaseDuration: time.Minute,
			}); err != nil {
				b.Errorf("Claim: %v", err)
				return
			}
		}
	})
}

// BenchmarkClaimWithAging measures the starvation-control ordering, whose
// expression cannot be served by jobs_claim_idx and therefore forces a sort.
// The gap against BenchmarkClaim/batch=20 is the price of aging.
func BenchmarkClaimWithAging(b *testing.B) {
	pool := benchPool(b)
	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()

	jobType := benchType(b)
	seedBench(b, pool, jobType, 50_000)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		got, err := repo.Claim(ctx, job.ClaimParams{
			WorkerID:      "bench",
			Limit:         20,
			LeaseDuration: time.Minute,
			Aging: job.AgingPolicy{
				Enabled:        true,
				BoostPerMinute: 1,
				MaxBoost:       50,
			},
		})
		if err != nil {
			b.Fatalf("Claim: %v", err)
		}
		if len(got) == 0 {
			b.StopTimer()
			resetBench(b, pool, jobType)
			b.StartTimer()
		}
	}
}

// BenchmarkCompleteAndFail measures the two fenced write paths.
func BenchmarkComplete(b *testing.B) {
	pool := benchPool(b)
	repo := jobpg.NewJobRepository(pool)
	ctx := context.Background()

	jobType := benchType(b)
	seedBench(b, pool, jobType, 100_000)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		b.StopTimer()
		claimed, err := repo.Claim(ctx, job.ClaimParams{
			WorkerID: "bench", Limit: 1, LeaseDuration: time.Minute,
		})
		if err != nil {
			b.Fatalf("Claim: %v", err)
		}
		if len(claimed) == 0 {
			resetBench(b, pool, jobType)
			b.StartTimer()
			continue
		}
		b.StartTimer()

		if err := repo.Complete(ctx, job.Lease{
			JobID:    claimed[0].ID,
			WorkerID: "bench",
			Epoch:    claimed[0].LeaseEpoch,
		}); err != nil {
			b.Fatalf("Complete: %v", err)
		}
	}
}
