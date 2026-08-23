// Package metrics defines every Prometheus collector in TaskForge.
//
// Collectors live in a struct that is constructed once and injected, not in
// package-level variables registered through init(). Globals would make the
// metrics untestable (two tests registering the same collector panic), would
// hide a dependency, and would tie every package that emits a metric to a
// single shared registry.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// namespace prefixes every metric, so `taskforge_` groups them in any shared
// Prometheus instance.
const namespace = "taskforge"

// Metrics holds every collector in the system.
//
// # A note on label cardinality
//
// Prometheus creates one time series per unique combination of label values,
// and each series costs memory in the server essentially forever. A label with
// unbounded values - a job id, a user id, a URL, an error message - produces
// unbounded series and will eventually take down your monitoring stack. This is
// the single most common way to break Prometheus.
//
// So the rule here is: labels must come from a small, closed set known at
// compile time.
//
//	job_type  - bounded by the handler registry (currently 4 values)
//	status    - bounded by the job state machine (5 values)
//	outcome   - success | failure | dead_letter
//	method    - HTTP methods
//	route     - the ROUTE PATTERN, never the URL. /api/v1/jobs/{id} is one
//	            series; /api/v1/jobs/<uuid> would be one series per job ever
//	            requested.
//
// Nothing here is labelled with a job id. When you need per-job detail, that is
// what logs and traces are for; metrics are for aggregates.
type Metrics struct {
	registry *prometheus.Registry

	// --- job lifecycle counters ---
	JobsCreated      *prometheus.CounterVec // by job_type
	JobsProcessed    *prometheus.CounterVec // by job_type, outcome
	JobsRetried      *prometheus.CounterVec // by job_type
	JobsDeadLettered *prometheus.CounterVec // by job_type, reason
	JobsReclaimed    prometheus.Counter     // recovered from expired leases

	// --- durations ---
	JobDuration *prometheus.HistogramVec // by job_type, outcome
	JobWait     *prometheus.HistogramVec // by job_type

	// --- gauges ---
	QueueDepth    *prometheus.GaugeVec // by status
	OldestPending prometheus.Gauge     // seconds
	ActiveWorkers prometheus.Gauge
	FleetCapacity prometheus.Gauge
	WorkerActive  *prometheus.GaugeVec // by worker_id: jobs in flight

	// --- HTTP ---
	HTTPRequests *prometheus.CounterVec   // by method, route, status
	HTTPDuration *prometheus.HistogramVec // by method, route
	HTTPInFlight prometheus.Gauge
}

// Option customises what a process exports.
type Option func(*options)

type options struct {
	fleetGauges bool
}

// WithFleetGauges registers the queue-depth and fleet gauges.
//
// Only ONE process should populate these, and only that process may export
// them. Registering them everywhere looked harmless right up until the alert
//
//	taskforge_active_workers == 0
//
// started firing permanently: every worker process was exporting its own
// unpopulated copy of the gauge, sitting at zero, and the alert matched those
// series. An exported metric that nobody sets is not a harmless zero, it is a
// lie with a timestamp on it.
//
// The API owns these gauges because it runs the collector that computes them.
func WithFleetGauges() Option {
	return func(o *options) { o.fleetGauges = true }
}

// New builds the collectors and registers them against a private registry.
func New(opts ...Option) *Metrics {
	var cfg options
	for _, opt := range opts {
		opt(&cfg)
	}

	reg := prometheus.NewRegistry()

	m := &Metrics{
		registry: reg,

		JobsCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_created_total",
			Help:      "Jobs accepted by the API, excluding idempotent duplicates.",
		}, []string{"job_type"}),

		JobsProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_processed_total",
			Help:      "Job executions that reached a terminal outcome.",
		}, []string{"job_type", "outcome"}),

		JobsRetried: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_retried_total",
			Help:      "Failed attempts that were rescheduled for another try.",
		}, []string{"job_type"}),

		JobsDeadLettered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_dead_lettered_total",
			Help:      "Jobs moved to the dead-letter queue.",
		}, []string{"job_type", "reason"}),

		JobsReclaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_reclaimed_total",
			Help:      "Jobs recovered from expired leases. A non-zero rate means workers are dying.",
		}),

		JobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "job_processing_duration_seconds",
			Help:      "Handler execution time.",
			// Buckets span 5ms to 5 minutes. Bucket boundaries are chosen for
			// the range you actually care about: the defaults top out at 10s,
			// which would lump every slow job into +Inf and make p99 useless.
			Buckets: []float64{0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300},
		}, []string{"job_type", "outcome"}),

		JobWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "job_wait_duration_seconds",
			Help:      "Time from a job becoming available to a worker claiming it. This is queue latency, and it is what users actually feel.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900},
		}, []string{"job_type"}),

		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_depth",
			Help:      "Jobs in each status.",
		}, []string{"status"}),

		OldestPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_oldest_pending_seconds",
			Help:      "Age of the oldest claimable job. Rising while workers are healthy means you are under-provisioned.",
		}),

		ActiveWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_workers",
			Help:      "Worker processes that have heartbeated recently.",
		}),

		FleetCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "fleet_capacity",
			Help:      "Total execution slots across live workers.",
		}),

		WorkerActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "worker_active_jobs",
			Help:      "Jobs currently executing in this worker process.",
		}, []string{"worker_id"}),

		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "HTTP requests by route pattern and status.",
		}, []string{"method", "route", "status"}),

		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "HTTP request latency.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "route"}),

		HTTPInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "http",
			Name:      "requests_in_flight",
			Help:      "HTTP requests currently being served.",
		}),
	}

	// Counters and histograms are safe to export from every process: Prometheus
	// aggregates them across instances, and a process that never increments one
	// simply contributes nothing to the sum.
	reg.MustRegister(
		m.JobsCreated, m.JobsProcessed, m.JobsRetried, m.JobsDeadLettered, m.JobsReclaimed,
		m.JobDuration, m.JobWait, m.WorkerActive,
		m.HTTPRequests, m.HTTPDuration, m.HTTPInFlight,
	)

	// Gauges describing global state are different: an unpopulated copy reads
	// as a real zero. They are created unconditionally so the struct is always
	// safe to use, but exported only by the process that maintains them.
	if cfg.fleetGauges {
		reg.MustRegister(m.QueueDepth, m.OldestPending, m.ActiveWorkers, m.FleetCapacity)
	}

	// Go runtime and process metrics: goroutine count, heap, GC pauses, open
	// file descriptors. These cost nothing and are the first things you want
	// when a service misbehaves - a goroutine count climbing without bound is
	// the classic leak signature.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return m
}

// Handler returns the /metrics HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		// A scrape must never take down the process it is measuring.
		Timeout: 10 * time.Second,
		// Report collector errors as HTTP 500 rather than serving a partial
		// scrape that silently looks like healthy zeroes.
		ErrorHandling: promhttp.HTTPErrorOnError,
	})
}

// Registry exposes the underlying registry, for tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Outcomes recorded on JobsProcessed and JobDuration.
const (
	OutcomeSuccess    = "success"
	OutcomeFailure    = "failure"
	OutcomeDeadLetter = "dead_letter"
)

// Reasons recorded on JobsDeadLettered.
const (
	ReasonRetriesExhausted = "retries_exhausted"
	ReasonPermanent        = "permanent_error"
)
