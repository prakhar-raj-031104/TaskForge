package middleware

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/httpx"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/logging"
)

// Logger writes one structured line per request and puts a request-scoped
// logger into the context.
//
// Two jobs in one middleware because they share the same derived logger:
// building it twice would mean the access log and the handler's own logs could
// drift apart on which fields they carry.
func Logger(base *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			reqLog := base.With(
				"request_id", httpx.RequestID(r.Context()),
				"method", r.Method,
				// r.URL.Path, never r.URL.RequestURI(): the query string
				// routinely carries tokens and filter values that should not be
				// duplicated into logs.
				"path", r.URL.Path,
			)

			// Handlers pull this out with logging.FromContext and inherit every
			// field above for free.
			ctx := logging.WithLogger(r.Context(), reqLog)

			rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r.WithContext(ctx))

			duration := time.Since(start)

			// Level by outcome: 5xx is our fault and deserves ERROR, 4xx is the
			// client's and is normal traffic at WARN, everything else is INFO.
			// Logging every 404 at ERROR is how alerting gets ignored.
			level := slog.LevelInfo
			switch {
			case rec.status >= http.StatusInternalServerError:
				level = slog.LevelError
			case rec.status >= http.StatusBadRequest:
				level = slog.LevelWarn
			}

			reqLog.LogAttrs(ctx, level, "http request",
				slog.Int("status", rec.status),
				slog.Int("bytes", rec.bytes),
				// Milliseconds as a float: dashboards want a number, and
				// slog.Duration would emit a Go duration string.
				slog.Float64("duration_ms", float64(duration.Microseconds())/1000),
				slog.String("remote_addr", clientIP(r)),
			)
		})
	}
}

// clientIP returns the address to attribute the request to.
//
// r.RemoteAddr is the only value that cannot be spoofed, but behind a proxy it
// is always the proxy. X-Forwarded-For is used ONLY when the deployment is
// known to sit behind a trusted proxy that overwrites it — otherwise any client
// can claim any address. This build has no proxy, so RemoteAddr it is, and the
// decision is written down rather than left implicit.
func clientIP(r *http.Request) string {
	return r.RemoteAddr
}
