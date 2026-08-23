package middleware

import (
	"context"
	"net/http"
	"time"
)

// Timeout gives every request a deadline on its context.
//
// This is NOT http.TimeoutHandler. That wrapper runs the handler in a separate
// goroutine and writes a 503 when the clock runs out, but the abandoned handler
// keeps running: its database query keeps holding a connection, and the
// goroutine leaks until the query finishes on its own.
//
// Setting a deadline on the context instead means the cancellation actually
// propagates. pgx checks ctx on every operation, so a query whose deadline
// passes is cancelled at the server too, freeing the backend rather than
// letting it grind on for a response nobody will read.
//
// The tradeoff: a handler that ignores its context can still overrun. That is
// why every call in this codebase takes a ctx and passes it down.
func Timeout(timeout time.Duration) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			// Without this the timer stays armed until it fires, holding the
			// context and everything it references. On a busy server that is a
			// steady, silent leak.
			defer cancel()

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
