// Package logging builds the application's structured logger.
//
// It deliberately takes plain strings rather than importing internal/config:
// a logging package that depends on the application's configuration type
// cannot be reused or tested in isolation. main is the only place that knows
// about both.
package logging

import (
	"io"
	"log/slog"
	"strings"
)

// New returns a slog.Logger writing to w.
//
// level is one of debug, info, warn, error. format is text or json; anything
// else falls back to text. Source positions are attached only at debug level,
// where they are worth the cost.
func New(w io.Writer, level, format string) *slog.Logger {
	lvl := ParseLevel(level)

	opts := &slog.HandlerOptions{
		Level:     lvl,
		AddSource: lvl <= slog.LevelDebug,
	}

	var h slog.Handler
	if strings.EqualFold(format, "json") {
		h = slog.NewJSONHandler(w, opts)
	} else {
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(h)
}

// ParseLevel maps a configuration string onto a slog.Level, defaulting to
// info for anything unrecognised.
func ParseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
