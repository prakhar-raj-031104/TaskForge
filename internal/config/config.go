// Package config loads every piece of runtime configuration for TaskForge from
// the process environment, validates it once at startup, and hands back an
// immutable Config value.
//
// Nothing else in the codebase is allowed to call os.Getenv. Configuration is
// read exactly once, in main, and then injected downwards as an explicit
// dependency. That keeps packages testable (you construct them with the values
// you want) and makes misconfiguration a startup failure rather than a
// surprise at 3am on the first request that happens to touch a bad setting.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Deployment environments.
const (
	EnvDevelopment = "development"
	EnvStaging     = "staging"
	EnvProduction  = "production"
)

// Config is the fully validated configuration for a TaskForge process. Both
// cmd/api and cmd/worker load the same struct; each simply uses the subset it
// cares about.
type Config struct {
	Env      string
	Log      Log
	HTTP     HTTP
	Database Database
	Worker   Worker
	Handlers Handlers
}

// Log controls the slog handler built in main.
type Log struct {
	Level  string // debug | info | warn | error
	Format string // text | json
}

// HTTP configures the API server. Every timeout here exists to stop a slow or
// malicious client from occupying a connection forever.
type HTTP struct {
	Addr            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
	MaxBodyBytes    int64

	// MetricsInterval is how often the API refreshes queue-depth gauges. It
	// should be at or below the Prometheus scrape interval, otherwise
	// consecutive scrapes read the same stale value.
	MetricsInterval time.Duration

	// CORSAllowedOrigins is the explicit set of browser origins permitted to
	// call this API cross-origin. Empty means CORS is off — every
	// cross-origin request is refused, which is the correct default for an
	// API with no auth layer in front of it.
	CORSAllowedOrigins []string
}

// Database configures the PostgreSQL connection pool.
type Database struct {
	Host            string
	Port            int
	User            string
	Password        string
	Name            string
	SSLMode         string
	MaxConns        int
	MinConns        int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	ConnectTimeout  time.Duration

	// RunMigrationsOnBoot applies embedded migrations at process startup.
	// Off by default: docker-compose.yml applies migrations as its own
	// deliberate one-shot step (see the comment on the migrate service
	// there), specifically so that several replicas booting at once never
	// race to apply the same migration. A single-instance PaaS deployment
	// has exactly one replica by construction, so that race cannot happen,
	// and there is no shell in the distroless image to run a separate
	// migrate container against — this is the escape hatch for that case.
	RunMigrationsOnBoot bool
}

// Worker configures a single worker process.
type Worker struct {
	// ID identifies this worker process in the database. When empty, the
	// worker derives one from hostname and PID.
	ID string
	// Concurrency is the number of jobs this process executes simultaneously,
	// i.e. the number of goroutines in its pool.
	Concurrency int
	// PollInterval is how often the worker asks the database for work when it
	// has spare capacity.
	PollInterval time.Duration
	// ShutdownTimeout bounds how long the process waits for in-flight jobs to
	// finish after receiving SIGTERM/SIGINT.
	ShutdownTimeout time.Duration

	// LeaseDuration is how long a worker owns a claimed job before another
	// worker may reclaim it. This is the visibility timeout.
	LeaseDuration time.Duration

	// JobTimeout bounds a single handler execution. It must be shorter than
	// LeaseDuration, otherwise a still-running job can be reclaimed and
	// executed a second time concurrently.
	JobTimeout time.Duration

	// Retry backoff: delay = Base * Factor^(attempt-1), capped at Max, then
	// jittered. See job.Backoff.
	BackoffBase   time.Duration
	BackoffMax    time.Duration
	BackoffFactor float64

	// ReaperInterval is how often this process looks for jobs whose lease has
	// expired because their worker died.
	//
	// This is the upper bound on how long a crashed worker's jobs stay
	// invisible, on top of the lease duration itself.
	ReaperInterval time.Duration

	// ReaperBatch bounds one reaper pass, keeping the transaction short.
	ReaperBatch int

	// HeartbeatInterval is how often a worker refreshes its inventory row.
	// The API presumes a worker dead after three missed beats.
	HeartbeatInterval time.Duration

	// WorkerRetention is how long a silent worker's row is kept before it is
	// pruned, so the table does not accumulate one row per container that has
	// ever existed.
	WorkerRetention time.Duration

	// Priority aging. Off by default because it trades an index-ordered scan
	// for a sort; see job.AgingPolicy.
	AgingEnabled        bool
	AgingBoostPerMinute float64
	AgingMaxBoost       float64

	// MetricsAddr is where the worker serves /metrics for Prometheus. A worker
	// has no API, but it still has to be scrapeable.
	MetricsAddr string
}

// Handlers configures the built-in job handlers.
type Handlers struct {
	// WebhookTimeout bounds one outbound HTTP attempt.
	WebhookTimeout time.Duration

	// WebhookAllowPrivateTargets permits webhook calls to loopback and private
	// addresses. Needed for local development; a server-side request forgery
	// hole in production.
	WebhookAllowPrivateTargets bool

	// EmailLatency simulates provider round-trip time in the stub sender.
	EmailLatency time.Duration
}

// IsProduction reports whether the process is running in the production
// environment. Used to pick safer defaults (JSON logs, no stack traces in
// responses).
func (c Config) IsProduction() bool { return c.Env == EnvProduction }

// LookupFunc has the same signature as os.LookupEnv. Taking it as a parameter
// instead of calling os.LookupEnv directly is what makes this package testable
// without mutating global process state.
type LookupFunc func(key string) (value string, ok bool)

// Load reads configuration from the process environment.
func Load() (Config, error) {
	return load(os.LookupEnv)
}

// load is the testable core of Load.
func load(lookup LookupFunc) (Config, error) {
	l := &loader{lookup: lookup}

	cfg := Config{
		Env: l.Enum("APP_ENV", EnvDevelopment, EnvDevelopment, EnvStaging, EnvProduction),
		Log: Log{
			Level:  l.Enum("LOG_LEVEL", "info", "debug", "info", "warn", "error"),
			Format: l.Enum("LOG_FORMAT", "text", "text", "json"),
		},
		HTTP: HTTP{
			Addr:            l.String("HTTP_ADDR", ":8080"),
			ReadTimeout:     l.Duration("HTTP_READ_TIMEOUT", 5*time.Second),
			WriteTimeout:    l.Duration("HTTP_WRITE_TIMEOUT", 10*time.Second),
			IdleTimeout:     l.Duration("HTTP_IDLE_TIMEOUT", 60*time.Second),
			ShutdownTimeout: l.Duration("HTTP_SHUTDOWN_TIMEOUT", 15*time.Second),
			MaxBodyBytes:       l.Int64("HTTP_MAX_BODY_BYTES", 64<<10), // 64 KiB
			MetricsInterval:    l.Duration("METRICS_REFRESH_INTERVAL", 10*time.Second),
			CORSAllowedOrigins: l.StringList("HTTP_CORS_ALLOWED_ORIGINS"),
		},
		Database: Database{
			Host:            l.String("DB_HOST", "localhost"),
			Port:            l.Int("DB_PORT", 5432),
			User:            l.String("DB_USER", "taskforge"),
			Password:        l.String("DB_PASSWORD", "taskforge"),
			Name:            l.String("DB_NAME", "taskforge"),
			SSLMode:         l.Enum("DB_SSLMODE", "disable", "disable", "require", "verify-ca", "verify-full"),
			MaxConns:        l.Int("DB_MAX_CONNS", 10),
			MinConns:        l.Int("DB_MIN_CONNS", 2),
			ConnMaxLifetime: l.Duration("DB_CONN_MAX_LIFETIME", 30*time.Minute),
			ConnMaxIdleTime: l.Duration("DB_CONN_MAX_IDLE_TIME", 5*time.Minute),
			ConnectTimeout:  l.Duration("DB_CONNECT_TIMEOUT", 5*time.Second),

			RunMigrationsOnBoot: l.Bool("DB_RUN_MIGRATIONS_ON_BOOT", false),
		},
		Worker: Worker{
			ID:                l.String("WORKER_ID", ""),
			Concurrency:       l.Int("WORKER_CONCURRENCY", 5),
			PollInterval:      l.Duration("WORKER_POLL_INTERVAL", time.Second),
			ShutdownTimeout:   l.Duration("WORKER_SHUTDOWN_TIMEOUT", 30*time.Second),
			LeaseDuration:     l.Duration("WORKER_LEASE_DURATION", 60*time.Second),
			JobTimeout:        l.Duration("WORKER_JOB_TIMEOUT", 30*time.Second),
			BackoffBase:       l.Duration("WORKER_BACKOFF_BASE", time.Second),
			BackoffMax:        l.Duration("WORKER_BACKOFF_MAX", 5*time.Minute),
			BackoffFactor:     l.Float64("WORKER_BACKOFF_FACTOR", 2),
			ReaperInterval:    l.Duration("WORKER_REAPER_INTERVAL", 30*time.Second),
			ReaperBatch:       l.Int("WORKER_REAPER_BATCH", 100),
			HeartbeatInterval: l.Duration("WORKER_HEARTBEAT_INTERVAL", 10*time.Second),
			WorkerRetention:   l.Duration("WORKER_RETENTION", time.Hour),

			AgingEnabled:        l.Bool("WORKER_AGING_ENABLED", false),
			AgingBoostPerMinute: l.Float64("WORKER_AGING_BOOST_PER_MINUTE", 1),
			AgingMaxBoost:       l.Float64("WORKER_AGING_MAX_BOOST", 50),
			MetricsAddr:         l.String("WORKER_METRICS_ADDR", ":9091"),
		},
		Handlers: Handlers{
			WebhookTimeout:             l.Duration("WEBHOOK_TIMEOUT", 10*time.Second),
			WebhookAllowPrivateTargets: l.Bool("WEBHOOK_ALLOW_PRIVATE_TARGETS", false),
			EmailLatency:               l.Duration("EMAIL_SIMULATED_LATENCY", 50*time.Millisecond),
		},
	}

	// Parse errors first: if DB_PORT was not a number there is no point
	// complaining that the port is out of range.
	if err := errors.Join(l.errs...); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

// validate enforces the invariants that parsing alone cannot catch.
func (c Config) validate() error {
	var errs []error

	if c.HTTP.Addr == "" {
		errs = append(errs, errors.New("HTTP_ADDR: must not be empty"))
	}
	if c.HTTP.MaxBodyBytes <= 0 {
		errs = append(errs, fmt.Errorf("HTTP_MAX_BODY_BYTES: must be positive, got %d", c.HTTP.MaxBodyBytes))
	}
	for _, t := range []struct {
		key string
		val time.Duration
	}{
		{"HTTP_READ_TIMEOUT", c.HTTP.ReadTimeout},
		{"HTTP_WRITE_TIMEOUT", c.HTTP.WriteTimeout},
		{"HTTP_IDLE_TIMEOUT", c.HTTP.IdleTimeout},
		{"HTTP_SHUTDOWN_TIMEOUT", c.HTTP.ShutdownTimeout},
		{"DB_CONNECT_TIMEOUT", c.Database.ConnectTimeout},
		{"WORKER_POLL_INTERVAL", c.Worker.PollInterval},
		{"WORKER_SHUTDOWN_TIMEOUT", c.Worker.ShutdownTimeout},
		{"WORKER_LEASE_DURATION", c.Worker.LeaseDuration},
		{"WORKER_JOB_TIMEOUT", c.Worker.JobTimeout},
		{"WORKER_BACKOFF_BASE", c.Worker.BackoffBase},
		{"WORKER_BACKOFF_MAX", c.Worker.BackoffMax},
		{"WORKER_REAPER_INTERVAL", c.Worker.ReaperInterval},
		{"WORKER_HEARTBEAT_INTERVAL", c.Worker.HeartbeatInterval},
		{"WORKER_RETENTION", c.Worker.WorkerRetention},
		{"WEBHOOK_TIMEOUT", c.Handlers.WebhookTimeout},
	} {
		if t.val <= 0 {
			errs = append(errs, fmt.Errorf("%s: must be greater than zero, got %s", t.key, t.val))
		}
	}

	if c.Database.Host == "" {
		errs = append(errs, errors.New("DB_HOST: must not be empty"))
	}
	if c.Database.Name == "" {
		errs = append(errs, errors.New("DB_NAME: must not be empty"))
	}
	if c.Database.Port < 1 || c.Database.Port > 65535 {
		errs = append(errs, fmt.Errorf("DB_PORT: must be between 1 and 65535, got %d", c.Database.Port))
	}
	if c.Database.MaxConns < 1 {
		errs = append(errs, fmt.Errorf("DB_MAX_CONNS: must be at least 1, got %d", c.Database.MaxConns))
	}
	if c.Database.MinConns < 0 {
		errs = append(errs, fmt.Errorf("DB_MIN_CONNS: must not be negative, got %d", c.Database.MinConns))
	}
	if c.Database.MinConns > c.Database.MaxConns {
		errs = append(errs, fmt.Errorf("DB_MIN_CONNS (%d): must not exceed DB_MAX_CONNS (%d)",
			c.Database.MinConns, c.Database.MaxConns))
	}
	if c.Worker.Concurrency < 1 {
		errs = append(errs, fmt.Errorf("WORKER_CONCURRENCY: must be at least 1, got %d", c.Worker.Concurrency))
	}
	if c.Worker.BackoffFactor <= 1 {
		errs = append(errs, fmt.Errorf("WORKER_BACKOFF_FACTOR: must be greater than 1, got %v", c.Worker.BackoffFactor))
	}
	if c.Worker.BackoffMax < c.Worker.BackoffBase {
		errs = append(errs, fmt.Errorf("WORKER_BACKOFF_MAX (%s): must not be less than WORKER_BACKOFF_BASE (%s)",
			c.Worker.BackoffMax, c.Worker.BackoffBase))
	}

	// The lease is renewed every LeaseDuration/3 while a job runs, so a long
	// job under a short lease is fine. The lease must simply be long enough
	// that a couple of missed renewals do not expire a healthy job.
	if c.Worker.LeaseDuration < 5*time.Second {
		errs = append(errs, fmt.Errorf(
			"WORKER_LEASE_DURATION: must be at least 5s so a transient database blip does not expire a healthy job's lease, got %s",
			c.Worker.LeaseDuration))
	}
	// The handler's own timeout must fire before the worker kills it, so the
	// failure is recorded with a useful message rather than a bare deadline.
	if c.Handlers.WebhookTimeout >= c.Worker.JobTimeout {
		errs = append(errs, fmt.Errorf(
			"WEBHOOK_TIMEOUT (%s): must be shorter than WORKER_JOB_TIMEOUT (%s)",
			c.Handlers.WebhookTimeout, c.Worker.JobTimeout))
	}

	// A worker needs a connection per concurrent job, plus headroom for the
	// claim query and the heartbeat. Sizing the pool below concurrency would
	// silently serialise jobs that look concurrent, which shows up as
	// mysteriously low throughput rather than as an error.
	if c.Database.MaxConns < c.Worker.Concurrency+1 {
		errs = append(errs, fmt.Errorf(
			"DB_MAX_CONNS (%d): must be at least WORKER_CONCURRENCY + 1 (%d), otherwise concurrent jobs queue for connections",
			c.Database.MaxConns, c.Worker.Concurrency+1))
	}

	return errors.Join(errs...)
}

// ---------------------------------------------------------------------------
// Connection string
// ---------------------------------------------------------------------------

// connURL builds the connection URL once so DSN and RedactedDSN can never
// drift apart.
func (d Database) connURL() *url.URL {
	q := url.Values{}
	q.Set("sslmode", d.SSLMode)
	if secs := int(d.ConnectTimeout.Seconds()); secs > 0 {
		q.Set("connect_timeout", strconv.Itoa(secs))
	}
	return &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(d.User, d.Password),
		Host:     net.JoinHostPort(d.Host, strconv.Itoa(d.Port)),
		Path:     "/" + d.Name,
		RawQuery: q.Encode(),
	}
}

// DSN returns the PostgreSQL connection string for pgx.
//
// It contains the password in cleartext. Never log this value; use
// RedactedDSN instead.
func (d Database) DSN() string { return d.connURL().String() }

// RedactedDSN returns the connection string with the password masked, safe for
// logs and error messages.
func (d Database) RedactedDSN() string { return d.connURL().Redacted() }

// LogValue implements slog.LogValuer.
//
// This is the reason a password can never leak into the logs by accident: even
// if somebody writes slog.Any("db", cfg.Database), slog calls this method and
// only the fields listed below are ever emitted.
func (d Database) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("host", d.Host),
		slog.Int("port", d.Port),
		slog.String("user", d.User),
		slog.String("name", d.Name),
		slog.String("sslmode", d.SSLMode),
		slog.Int("max_conns", d.MaxConns),
		slog.Int("min_conns", d.MinConns),
	)
}

// LogValue implements slog.LogValuer for the whole configuration.
func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("env", c.Env),
		slog.Group("log",
			slog.String("level", c.Log.Level),
			slog.String("format", c.Log.Format),
		),
		slog.Group("http",
			slog.String("addr", c.HTTP.Addr),
			slog.Duration("read_timeout", c.HTTP.ReadTimeout),
			slog.Duration("write_timeout", c.HTTP.WriteTimeout),
			slog.Duration("shutdown_timeout", c.HTTP.ShutdownTimeout),
			slog.Int64("max_body_bytes", c.HTTP.MaxBodyBytes),
		),
		slog.Any("database", c.Database),
		slog.Group("worker",
			slog.Int("concurrency", c.Worker.Concurrency),
			slog.Duration("poll_interval", c.Worker.PollInterval),
			slog.Duration("shutdown_timeout", c.Worker.ShutdownTimeout),
			slog.Duration("lease_duration", c.Worker.LeaseDuration),
			slog.Duration("job_timeout", c.Worker.JobTimeout),
			slog.Duration("backoff_base", c.Worker.BackoffBase),
			slog.Duration("backoff_max", c.Worker.BackoffMax),
			slog.Float64("backoff_factor", c.Worker.BackoffFactor),
		),
	)
}

// ---------------------------------------------------------------------------
// loader
// ---------------------------------------------------------------------------

// loader reads typed values from the environment and accumulates every parse
// error it encounters. Accumulating rather than returning on the first failure
// means a developer with three typos in their .env sees all three at once.
type loader struct {
	lookup LookupFunc
	errs   []error
}

func (l *loader) fail(key string, format string, args ...any) {
	l.errs = append(l.errs, fmt.Errorf("%s: %s", key, fmt.Sprintf(format, args...)))
}

// String returns the value of key, or def when the variable is unset or empty.
// An empty value is deliberately treated as unset: `DB_HOST=` in a .env file
// is a mistake, not a request for an empty host.
func (l *loader) String(key, def string) string {
	if v, ok := l.lookup(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

// StringList returns key split on commas, trimming whitespace around each
// element and dropping empty ones. An unset variable yields an empty (not
// nil) slice, so callers can range over it without a nil check.
func (l *loader) StringList(key string) []string {
	raw := l.String(key, "")
	if raw == "" {
		return []string{}
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Enum returns the value of key, requiring it to be one of allowed.
func (l *loader) Enum(key, def string, allowed ...string) string {
	v := l.String(key, def)
	if !slices.Contains(allowed, v) {
		l.fail(key, "must be one of [%s], got %q", strings.Join(allowed, " "), v)
		return def
	}
	return v
}

// Int returns the value of key parsed as a base-10 integer.
func (l *loader) Int(key string, def int) int {
	raw := l.String(key, "")
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		l.fail(key, "must be an integer, got %q", raw)
		return def
	}
	return n
}

// Int64 returns the value of key parsed as a base-10 64-bit integer.
func (l *loader) Int64(key string, def int64) int64 {
	raw := l.String(key, "")
	if raw == "" {
		return def
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		l.fail(key, "must be an integer, got %q", raw)
		return def
	}
	return n
}

// Float64 returns the value of key parsed as a floating-point number.
func (l *loader) Float64(key string, def float64) float64 {
	raw := l.String(key, "")
	if raw == "" {
		return def
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		l.fail(key, "must be a number, got %q", raw)
		return def
	}
	return f
}

// Bool returns the value of key parsed as a boolean.
//
// strconv.ParseBool accepts 1, t, T, TRUE, true, True and their false
// counterparts, which covers every spelling people actually put in a .env file.
func (l *loader) Bool(key string, def bool) bool {
	raw := l.String(key, "")
	if raw == "" {
		return def
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		l.fail(key, "must be true or false, got %q", raw)
		return def
	}
	return b
}

// Duration returns the value of key parsed as a Go duration such as
// "500ms", "5s" or "1m30s".
func (l *loader) Duration(key string, def time.Duration) time.Duration {
	raw := l.String(key, "")
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		l.fail(key, "must be a duration such as 500ms, 5s or 1m30s, got %q", raw)
		return def
	}
	return d
}
