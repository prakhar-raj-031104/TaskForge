// Package database owns the PostgreSQL connection pool.
//
// It deliberately does not import internal/config. The pool needs a DSN and a
// handful of sizing knobs, not the application's whole configuration type;
// keeping it that way means this package can be used from tests and tools that
// have no Config at all.
package database

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Options tunes the connection pool. Zero values mean "leave the pgx default
// alone", so callers only set what they care about.
type Options struct {
	// MaxConns is the hard ceiling on connections this process will open.
	//
	// This is the single most important number in the system for database
	// health: total connections to PostgreSQL is MaxConns x (api replicas +
	// worker replicas). PostgreSQL's own max_connections defaults to 100, and
	// every connection costs a backend process and its memory. Sizing pools
	// per-process without doing that multiplication is how teams take down
	// their database during a routine scale-up.
	MaxConns int

	// MinConns is the number of idle connections kept warm. Connection setup
	// includes a TCP handshake plus authentication, which is expensive enough
	// that a queue polling every second should not pay it repeatedly.
	MinConns int

	// ConnMaxLifetime bounds how long a connection may live. Recycling
	// connections lets a load balancer or failover actually take effect, and
	// caps per-backend memory growth.
	ConnMaxLifetime time.Duration

	// ConnMaxIdleTime closes connections that have been unused this long,
	// letting the pool shrink back towards MinConns after a burst.
	ConnMaxIdleTime time.Duration

	// AppName appears in pg_stat_activity.application_name. With api and
	// worker processes sharing a database, this is what lets you answer "which
	// binary is holding that lock" during an incident.
	AppName string
}

// NewPool creates a connection pool and verifies it can reach the database.
//
// It fails fast: a process that cannot reach PostgreSQL at startup exits
// rather than serving traffic it cannot fulfil. The supervisor (Compose,
// Kubernetes) restarts it, and the readiness endpoint keeps it out of rotation
// until the database is genuinely available.
func NewPool(ctx context.Context, dsn string, opts Options) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		// Note: err from ParseConfig can contain the DSN, so callers should
		// pass a DSN they are willing to see in logs, or wrap this error.
		return nil, fmt.Errorf("database: parse dsn: %w", err)
	}

	if opts.MaxConns > 0 {
		n, err := toInt32(opts.MaxConns)
		if err != nil {
			return nil, fmt.Errorf("database: max conns: %w", err)
		}
		cfg.MaxConns = n
	}
	if opts.MinConns > 0 {
		n, err := toInt32(opts.MinConns)
		if err != nil {
			return nil, fmt.Errorf("database: min conns: %w", err)
		}
		cfg.MinConns = n
	}
	if opts.ConnMaxLifetime > 0 {
		cfg.MaxConnLifetime = opts.ConnMaxLifetime
	}
	if opts.ConnMaxIdleTime > 0 {
		cfg.MaxConnIdleTime = opts.ConnMaxIdleTime
	}

	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	if opts.AppName != "" {
		cfg.ConnConfig.RuntimeParams["application_name"] = opts.AppName
	}
	// Belt and braces alongside TZ/PGTZ in Compose: every session this pool
	// opens computes now() and interval arithmetic in UTC, whatever the host
	// or container thinks the local zone is. Lease deadlines are absolute
	// instants and must never depend on a machine's locale.
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("database: create pool: %w", err)
	}

	// NewWithConfig is lazy: it does not open a connection. Ping forces one so
	// that bad credentials or an unreachable host surface here, at startup,
	// rather than on the first request.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("database: ping: %w", err)
	}

	return pool, nil
}

// Pinger is the small slice of *pgxpool.Pool that a health check needs.
//
// Declaring the interface here, in the package that consumes it rather than
// the package that implements it, is the idiomatic Go direction: handlers can
// depend on this one-method interface and be tested with a two-line fake.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Check reports whether the database is reachable within the given timeout.
func Check(ctx context.Context, p Pinger, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := p.Ping(ctx); err != nil {
		return fmt.Errorf("database unreachable: %w", err)
	}
	return nil
}

func toInt32(n int) (int32, error) {
	if n < 0 || n > math.MaxInt32 {
		return 0, fmt.Errorf("value %d out of range for int32", n)
	}
	return int32(n), nil
}
