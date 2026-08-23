package middleware

import (
	"bytes"
	"context"
	"net/http"
	"strings"

	"github.com/anjani-kr-singh-ai/taskforge/internal/httpx"
)

// JSONErrors rewrites net/http's built-in 404 and 405 responses into the API's
// JSON error envelope.
//
// Why this exists instead of a catch-all "/" route:
//
// http.ServeMux produces 405 Method Not Allowed, complete with an Allow header,
// when a request's path matches a registered pattern but its method does not.
// That only happens if NOTHING else matches. Registering a catch-all
// mux.HandleFunc("/", ...) to serve a pretty 404 therefore silently destroys
// 405 handling: "/" matches every request, so a DELETE to a GET-only route
// falls through to the catch-all and reports 404 — telling the client the
// endpoint does not exist when it does.
//
// Keeping the mux free of a catch-all preserves correct status codes, and this
// middleware supplies the JSON body the standard library does not.
func JSONErrors() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ew := &envelopeWriter{ResponseWriter: w, ctx: r.Context()}
			next.ServeHTTP(ew, r)
			ew.finish()
		})
	}
}

// envelopeWriter withholds the response until it knows whether the body needs
// replacing.
type envelopeWriter struct {
	http.ResponseWriter
	ctx context.Context

	status      int
	wroteHeader bool
	intercept   bool
	discarded   bytes.Buffer
}

func (w *envelopeWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = status

	// Intercept ONLY the standard library's plain-text 404/405. Our own
	// handlers set a JSON content type before writing their status, so a
	// genuine "job not found" response passes straight through untouched.
	contentType := w.Header().Get("Content-Type")
	if (status == http.StatusNotFound || status == http.StatusMethodNotAllowed) &&
		!strings.HasPrefix(contentType, "application/json") {
		w.intercept = true
		// Deliberately do NOT call the wrapped WriteHeader yet: once the status
		// line is on the wire it cannot be changed, and we still need the
		// header map mutable to replace Content-Type and Content-Length.
		return
	}

	w.ResponseWriter.WriteHeader(status)
}

func (w *envelopeWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if w.intercept {
		// Swallow "404 page not found\n" and report success so the caller sees
		// nothing unusual.
		return w.discarded.Write(b)
	}
	return w.ResponseWriter.Write(b)
}

func (w *envelopeWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// finish emits the replacement body, if any.
func (w *envelopeWriter) finish() {
	if !w.intercept {
		return
	}

	code, message := httpx.CodeNotFound, "the requested endpoint does not exist"
	if w.status == http.StatusMethodNotAllowed {
		code = httpx.CodeMethodNotAllowed
		// The Allow header ServeMux set is still in the header map and is
		// preserved, so the client is told which methods it should have used.
		message = "that method is not allowed on this endpoint"
		if allow := w.Header().Get("Allow"); allow != "" {
			message += " (allowed: " + allow + ")"
		}
	}

	httpx.WriteError(w.ctx, w.ResponseWriter, w.status, code, message)
}
