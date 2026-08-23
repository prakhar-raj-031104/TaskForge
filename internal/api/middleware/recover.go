package middleware

import (
	"errors"
	"net/http"
	"runtime/debug"

	"github.com/anjani-kr-singh-ai/taskforge/internal/httpx"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/logging"
)

// Recover turns a panic in a handler into a 500 instead of a dead process.
//
// Go's http server already recovers panics per connection, but its recovery
// closes the connection without a response, so the client sees a transport
// error rather than a status code, and nothing is logged in our format. Doing
// it ourselves means a bug in one handler produces one clean 500 with a request
// id, and the other in-flight requests are untouched.
//
// Note this does NOT make panics acceptable. It is a blast-radius limiter; the
// stack trace it logs is meant to be treated as a bug report.
func Recover() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// defer runs during panic unwinding, which is the only place
			// recover() has any effect. Calling recover() outside a deferred
			// function returns nil and does nothing.
			defer func() {
				rec := recover()
				if rec == nil {
					return
				}

				// http.ErrAbortHandler is the documented way for a handler to
				// abandon a response deliberately. Re-panic so the server
				// handles it as intended rather than logging a false alarm.
				if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(rec)
				}

				ctx := r.Context()
				logging.FromContext(ctx).ErrorContext(ctx, "panic recovered in http handler",
					"panic", rec,
					// The stack is the whole point: without it you know a
					// handler died but not where.
					"stack", string(debug.Stack()),
				)

				// If the handler already sent a status there is nothing left to
				// write; attempting it would just log "superfluous WriteHeader".
				if rr, ok := w.(*responseRecorder); ok && rr.wroteHeader {
					return
				}

				httpx.WriteInternalError(ctx, w)
			}()

			next.ServeHTTP(w, r)
		})
	}
}
