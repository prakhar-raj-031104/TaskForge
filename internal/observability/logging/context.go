package logging

import (
	"context"
	"log/slog"
)

type contextKey int

const loggerKey contextKey = iota

// WithLogger returns a copy of ctx carrying log.
//
// This is how per-request fields reach code that has no idea it is serving a
// request: the middleware attaches a logger already tagged with request_id,
// and a repository three layers down logs with that tag automatically. The
// alternative — threading a *slog.Logger through every function signature —
// pollutes every API in the codebase.
func WithLogger(ctx context.Context, log *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, log)
}

// FromContext returns the logger stored in ctx, falling back to slog.Default.
//
// It never returns nil. Logging is not allowed to be the thing that panics a
// request handler, so the failure mode here is "less context in the log line",
// never "nil pointer dereference".
func FromContext(ctx context.Context) *slog.Logger {
	if log, ok := ctx.Value(loggerKey).(*slog.Logger); ok && log != nil {
		return log
	}
	return slog.Default()
}
