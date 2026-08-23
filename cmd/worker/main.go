// Command worker is the TaskForge job execution process.
//
// Run as many of these as you like: they compete for work safely through
// PostgreSQL's FOR UPDATE SKIP LOCKED, so scaling out is "start another one".
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/anjani-kr-singh-ai/taskforge/internal/config"
	"github.com/anjani-kr-singh-ai/taskforge/internal/database"
	"github.com/anjani-kr-singh-ai/taskforge/internal/handlers"
	"github.com/anjani-kr-singh-ai/taskforge/internal/httpx"
	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
	jobpg "github.com/anjani-kr-singh-ai/taskforge/internal/job/postgres"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/logging"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/metrics"
	"github.com/anjani-kr-singh-ai/taskforge/internal/worker"
)

const appName = "taskforge-worker"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "worker: fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log := logging.New(os.Stdout, cfg.Log.Level, cfg.Log.Format)

	id := workerID(cfg.Worker.ID)
	log.Info("taskforge worker starting", "worker_id", id, "config", cfg)

	// Cancelled on SIGINT (Ctrl+C) or SIGTERM (what Docker and Kubernetes send).
	// Everything downstream watches this one context, which is what makes
	// graceful shutdown a single mechanism rather than several ad-hoc ones.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx, cancel := context.WithTimeout(ctx, cfg.Database.ConnectTimeout+5*time.Second)
	defer cancel()

	pool, err := database.NewPool(startupCtx, cfg.Database.DSN(), database.Options{
		MaxConns:        cfg.Database.MaxConns,
		MinConns:        cfg.Database.MinConns,
		ConnMaxLifetime: cfg.Database.ConnMaxLifetime,
		ConnMaxIdleTime: cfg.Database.ConnMaxIdleTime,
		// Include the worker id so pg_stat_activity points at one container.
		AppName: appName + "/" + id,
	})
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", cfg.Database.RedactedDSN(), err)
	}
	defer pool.Close()

	log.Info("database connected", "worker_id", id, "dsn", cfg.Database.RedactedDSN())

	registry, err := buildRegistry(cfg)
	if err != nil {
		return err
	}

	queue := jobpg.NewJobRepository(pool)
	m := metrics.New()

	w, err := worker.New(worker.Config{
		ID:              id,
		Concurrency:     cfg.Worker.Concurrency,
		PollInterval:    cfg.Worker.PollInterval,
		LeaseDuration:   cfg.Worker.LeaseDuration,
		JobTimeout:      cfg.Worker.JobTimeout,
		ShutdownTimeout: cfg.Worker.ShutdownTimeout,
		Backoff: job.Backoff{
			Base:   cfg.Worker.BackoffBase,
			Max:    cfg.Worker.BackoffMax,
			Factor: cfg.Worker.BackoffFactor,
		},
		Aging: job.AgingPolicy{
			Enabled:        cfg.Worker.AgingEnabled,
			BoostPerMinute: cfg.Worker.AgingBoostPerMinute,
			MaxBoost:       cfg.Worker.AgingMaxBoost,
		},
		Metrics: m,
	}, queue, registry, log)
	if err != nil {
		return err
	}

	reaper := worker.NewReaper(queue, cfg.Worker.ReaperInterval, cfg.Worker.ReaperBatch, m,
		log.With("worker_id", id, "component", "reaper"))

	heartbeat := worker.NewHeartbeat(queue, worker.HeartbeatConfig{
		WorkerID:    id,
		Concurrency: cfg.Worker.Concurrency,
		Interval:    cfg.Worker.HeartbeatInterval,
		Retention:   cfg.Worker.WorkerRetention,
		// Reads an atomic counter inside the worker, so the inventory shows
		// live utilisation rather than just configured capacity.
		ActiveJobs: w.ActiveJobs,
	}, log.With("worker_id", id, "component", "heartbeat"))

	// errgroup runs all three loops and waits for all of them. Its key property
	// here is that if any returns an error, the derived context is cancelled
	// and the others are asked to stop too, so the process never ends up
	// half-running with a dead reaper and a live claim loop.
	// A worker has no API, but Prometheus still has to reach it, so it runs a
	// tiny HTTP server exposing /metrics and a liveness endpoint on its own
	// port. Keeping it separate from the API's port means the two can be
	// scraped and firewalled independently.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", m.Handler())
	metricsMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	metricsSrv := httpx.NewServer(httpx.ServerConfig{
		Addr:         cfg.Worker.MetricsAddr,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}, metricsMux, log.With("component", "metrics"))

	g, groupCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return w.Run(groupCtx) })
	g.Go(func() error { return reaper.Run(groupCtx) })
	g.Go(func() error { return heartbeat.Run(groupCtx) })
	g.Go(func() error { return httpx.Serve(groupCtx, metricsSrv, 5*time.Second, log) })

	// Blocks until the signal arrives, then drains in-flight jobs.
	if err := g.Wait(); err != nil {
		return err
	}

	log.Info("worker shut down cleanly", "worker_id", id)
	return nil
}

// buildRegistry is the one place that knows which job types exist.
//
// Adding a job type is a new handler in internal/handlers plus one Register
// call here. The worker core, the API and the database schema are all
// untouched, which is exactly what the Handler interface is for.
func buildRegistry(cfg config.Config) (*job.Registry, error) {
	registry := job.NewRegistry()

	registrations := []struct {
		jobType string
		handler job.Handler
	}{
		{handlers.TypeSleep, handlers.NewSleep()},
		{handlers.TypeSendEmail, handlers.NewSendEmail(handlers.LogSender{
			Latency: cfg.Handlers.EmailLatency,
		})},
		{handlers.TypeWebhook, handlers.NewWebhook(handlers.WebhookOptions{
			Timeout:             cfg.Handlers.WebhookTimeout,
			AllowPrivateTargets: cfg.Handlers.WebhookAllowPrivateTargets,
		})},
		{handlers.TypeFlaky, handlers.NewFlaky()},
	}

	for _, r := range registrations {
		if err := registry.Register(r.jobType, r.handler); err != nil {
			return nil, fmt.Errorf("registering handlers: %w", err)
		}
	}

	return registry, nil
}

// workerID returns the configured identity, or derives a stable-per-process one
// from hostname and PID. In Docker the hostname is the container ID, which
// makes the derived value both unique across the cluster and traceable back to
// a specific container.
func workerID(configured string) string {
	if configured != "" {
		return configured
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
