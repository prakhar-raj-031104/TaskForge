// Command api is the TaskForge HTTP API server.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/anjani-kr-singh-ai/taskforge/internal/api"
	"github.com/anjani-kr-singh-ai/taskforge/internal/config"
	"github.com/anjani-kr-singh-ai/taskforge/internal/database"
	"github.com/anjani-kr-singh-ai/taskforge/internal/httpx"
	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
	jobpg "github.com/anjani-kr-singh-ai/taskforge/internal/job/postgres"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/logging"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/metrics"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/queuemetrics"
)

// appName lands in pg_stat_activity.application_name, so a DBA looking at a
// long-running query can tell the API apart from a worker.
const appName = "taskforge-api"

// main does exactly two things: call run, and translate its error into an exit
// code. Keeping it this thin means every piece of real startup logic lives in
// a function that returns an error and can therefore be tested — and it is why
// you will not find log.Fatal or panic anywhere else in this codebase.
// healthcheck lets the binary probe itself: `/app -healthcheck`.
//
// The runtime image is distroless, so it contains no shell, no curl and no
// wget. That is the point of distroless, and it means a container healthcheck
// has to be something already in the image. The binary is already there, so it
// probes its own readiness endpoint and reports through its exit code.
var healthcheck = flag.Bool("healthcheck", false,
	"probe the local readiness endpoint and exit 0 if healthy")

func main() {
	flag.Parse()

	if *healthcheck {
		if err := probe(); err != nil {
			fmt.Fprintf(os.Stderr, "api: unhealthy: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "api: fatal: %v\n", err)
		os.Exit(1)
	}
}

// probe requests the readiness endpoint on the configured address.
func probe() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// HTTP_ADDR is typically ":8080", which is not a dialable URL host.
	addr := cfg.HTTP.Addr
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + addr + "/api/v1/ready")
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("readiness returned %d", resp.StatusCode)
	}
	return nil
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log := logging.New(os.Stdout, cfg.Log.Level, cfg.Log.Format)
	log.Info("taskforge api starting", "config", cfg)

	// NotifyContext returns a context that is cancelled when one of these
	// signals arrives. This one line replaces the usual signal.Notify plus
	// channel plus goroutine dance, and it gives us cancellation in the same
	// currency every other API in the program already speaks.
	//
	//   SIGINT  — Ctrl+C in a terminal
	//   SIGTERM — what Docker, Compose and Kubernetes send to ask a container
	//             to stop. Ignoring it means being SIGKILLed 10 seconds later,
	//             mid-request, with no chance to drain.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// Restores the default signal behaviour, so a second Ctrl+C during a slow
	// shutdown kills the process outright instead of being swallowed.
	defer stop()

	// Startup gets its own bounded context. Without a deadline, an unreachable
	// database host would leave the process hanging in "starting" forever,
	// which is far harder to diagnose than a clean failure.
	startupCtx, cancel := context.WithTimeout(ctx, cfg.Database.ConnectTimeout+5*time.Second)
	defer cancel()

	pool, err := database.NewPool(startupCtx, cfg.Database.DSN(), database.Options{
		MaxConns:        cfg.Database.MaxConns,
		MinConns:        cfg.Database.MinConns,
		ConnMaxLifetime: cfg.Database.ConnMaxLifetime,
		ConnMaxIdleTime: cfg.Database.ConnMaxIdleTime,
		AppName:         appName,
	})
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", cfg.Database.RedactedDSN(), err)
	}
	// defer runs when run returns, which is exactly the point of keeping the
	// real work out of main: os.Exit would skip this.
	defer pool.Close()

	log.Info("database connected", "dsn", cfg.Database.RedactedDSN())

	// Dependency injection, by hand, in one place. The repository knows about
	// pgx, the service knows about the repository interface, the router knows
	// about the service. No globals, no container, no init() magic — you can
	// read the entire object graph in these three statements.
	repo := jobpg.NewJobRepository(pool)
	// The API owns the fleet and queue-depth gauges, because it runs the
	// collector that populates them. Worker processes deliberately do not
	// export those series; see metrics.WithFleetGauges.
	m := metrics.New(metrics.WithFleetGauges())
	jobService := job.NewService(repo, log, job.WithMetrics(m))

	router := api.NewRouter(api.Config{
		MaxBodyBytes: cfg.HTTP.MaxBodyBytes,
		// Slightly under WriteTimeout so a handler's own deadline fires first
		// and produces a proper JSON error, rather than the server cutting the
		// connection with no response at all.
		RequestTimeout: cfg.HTTP.WriteTimeout - time.Second,
		// Three missed heartbeats before a worker is presumed dead. One missed
		// beat is a hiccup; three is a pattern. Deriving this from the interval
		// means tuning the interval cannot accidentally mark the whole fleet
		// dead.
		WorkerStaleAfter: 3 * cfg.Worker.HeartbeatInterval,
	}, api.Deps{
		Jobs:    jobService,
		Workers: repo,
		DB:      pool,
		Metrics: m,
		Log:     log,
	})

	srv := httpx.NewServer(httpx.ServerConfig{
		Addr:         cfg.HTTP.Addr,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
		IdleTimeout:  cfg.HTTP.IdleTimeout,
	}, router, log)

	// Queue-depth gauges are refreshed on a timer rather than during the
	// Prometheus scrape, so a slow database can never make scrapes time out and
	// leave a hole in the graphs at exactly the moment you need them.
	collector := queuemetrics.New(repo, m,
		cfg.HTTP.MetricsInterval,
		3*cfg.Worker.HeartbeatInterval,
		log.With("component", "queue_collector"))

	g, groupCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return httpx.Serve(groupCtx, srv, cfg.HTTP.ShutdownTimeout, log) })
	g.Go(func() error { return collector.Run(groupCtx) })

	// Blocks until a signal arrives, then drains in-flight requests.
	if err := g.Wait(); err != nil {
		return err
	}

	log.Info("api shut down cleanly")
	return nil
}
