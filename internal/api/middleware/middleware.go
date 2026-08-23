// Package middleware holds the cross-cutting HTTP concerns: request
// identifiers, structured access logs, panic recovery and per-request
// deadlines.
//
// A middleware in Go is just a function that takes an http.Handler and returns
// a new one that does something extra before or after calling it. There is no
// framework and no registration mechanism — the whole idea is one type
// declaration and one loop.
package middleware

import "net/http"

// Middleware wraps a handler with additional behaviour.
type Middleware func(http.Handler) http.Handler

// Chain applies middleware to h so that the FIRST argument is the OUTERMOST
// wrapper.
//
//	Chain(h, A, B, C)  ==  A(B(C(h)))
//
// A request therefore travels A -> B -> C -> h on the way in, and back out in
// reverse. The loop runs backwards precisely to produce that reading order:
// listing middleware in the order a request meets them is far easier to reason
// about than nesting the calls by hand.
func Chain(h http.Handler, mw ...Middleware) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

// responseRecorder wraps http.ResponseWriter to remember the status code and
// byte count.
//
// It exists because http.ResponseWriter is write-only: once a handler calls
// WriteHeader(404) there is no way to ask what it wrote. Access logging and
// metrics both need that number, so we intercept it.
type responseRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int
	wroteHeader bool
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.wroteHeader {
		// The handler already committed a status. Calling WriteHeader twice is
		// a bug in the handler; swallowing it here keeps the log honest about
		// what the client actually received.
		return
	}
	r.status = status
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	// A handler that calls Write without WriteHeader implicitly sends 200.
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer, so wrapping
// does not break Flush, Hijack or SetWriteDeadline. Without this, adding an
// access log would silently break streaming responses — a classic
// middleware-wrapping bug.
func (r *responseRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
