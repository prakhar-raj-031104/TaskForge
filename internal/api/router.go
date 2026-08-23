// Package api wires handlers and middleware into a single http.Handler.
package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/api/handler"
	"github.com/anjani-kr-singh-ai/taskforge/internal/api/middleware"
	"github.com/anjani-kr-singh-ai/taskforge/internal/database"
	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/metrics"
)

// Config carries what the router needs that is not a dependency.
type Config struct {
	MaxBodyBytes   int64
	RequestTimeout time.Duration
	// WorkerStaleAfter is how long without a heartbeat before a worker is
	// reported as not alive. Derived from the heartbeat interval by the caller.
	WorkerStaleAfter time.Duration
}

// Deps are the router's collaborators.
type Deps struct {
	Jobs    *job.Service
	Workers job.WorkerRegistry
	DB      database.Pinger
	Metrics *metrics.Metrics
	Log     *slog.Logger
}

// NewRouter builds the fully wrapped API handler.
func NewRouter(cfg Config, deps Deps) http.Handler {
	// http.ServeMux gained method and wildcard patterns in Go 1.22. Before
	// that, "POST /api/v1/jobs/{id}" needed a third-party router; now the
	// standard library covers everything this API does, so there is no reason
	// to take the dependency.
	mux := http.NewServeMux()

	jobs := handler.NewJobs(deps.Jobs, cfg.MaxBodyBytes)
	deadLetter := handler.NewDeadLetter(deps.Jobs)
	workers := handler.NewWorkers(deps.Workers, cfg.WorkerStaleAfter)
	health := handler.NewHealth(deps.DB)

	// Patterns are "METHOD /path". A request with the right path but the wrong
	// method gets 405 with an Allow header automatically, which previously had
	// to be written by hand in every handler.
	mux.HandleFunc("POST /api/v1/jobs", jobs.Create)
	mux.HandleFunc("GET /api/v1/jobs", jobs.List)
	mux.HandleFunc("GET /api/v1/jobs/{id}", jobs.Get)

	mux.HandleFunc("GET /api/v1/dead-letter-jobs", deadLetter.List)
	mux.HandleFunc("POST /api/v1/dead-letter-jobs/{id}/retry", deadLetter.Retry)

	mux.HandleFunc("GET /api/v1/workers", workers.List)

	mux.HandleFunc("GET /api/v1/health", health.Live)
	mux.HandleFunc("GET /api/v1/ready", health.Ready)

	// Unversioned and outside /api/v1 by convention: /metrics is an operational
	// endpoint for Prometheus, not part of the public API contract, and it
	// should never be versioned alongside it.
	mux.Handle("GET /metrics", deps.Metrics.Handler())

	// Note there is deliberately NO mux.HandleFunc("/", ...) catch-all. It
	// would match every request and so suppress ServeMux's built-in 405
	// handling; middleware.JSONErrors supplies the JSON body instead. See the
	// comment on that function.

	// Order is the order a request travels:
	//
	//   RequestID   assign the id first, so every later layer can log it
	//   Logger      wrap the writer and start the timer, so it observes the
	//               final status including one written by Recover
	//   Recover     inside Logger, so a panic still produces one access log line
	//   Metrics     after Recover so a panic is counted as the 500 it becomes
	//   Timeout     bounds handler work, not time spent logging
	//   JSONErrors  innermost, wrapping the mux itself, because the responses
	//               it rewrites are produced by the mux
	return middleware.Chain(mux,
		middleware.RequestID(),
		middleware.Logger(deps.Log),
		middleware.Recover(),
		// The resolver closes over the mux so the metrics label is the route
		// pattern rather than the raw path. See middleware.Metrics for why
		// reading r.Pattern here would not work.
		middleware.Metrics(deps.Metrics, func(r *http.Request) string {
			_, pattern := mux.Handler(r)
			return pattern
		}),
		middleware.Timeout(cfg.RequestTimeout),
		middleware.JSONErrors(),
	)
}
