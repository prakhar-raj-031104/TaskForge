// Package httpx holds transport-level helpers shared by the API: JSON
// encoding and decoding, the error envelope, request-scoped context values,
// and the server lifecycle.
//
// It knows nothing about jobs. Keeping it domain-free is what lets the worker
// reuse the same server bootstrap for its metrics endpoint.
package httpx

import "context"

// contextKey is an unexported named type used for context keys.
//
// Using a plain string here would be a real bug, not a style preference:
// context keys are compared by value AND type, so any other package storing
// the string "request_id" would collide with ours and silently overwrite it.
// An unexported type makes collision impossible, because no other package can
// even name the type.
type contextKey int

const (
	requestIDKey contextKey = iota
)

// WithRequestID returns a copy of ctx carrying the request id.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestID returns the request id stored in ctx, or "" when absent.
//
// The comma-ok type assertion matters: ctx.Value returns any, and a bare
// assertion would panic on a request that somehow bypassed the middleware.
// Observability code must never be the thing that takes down a request.
func RequestID(ctx context.Context) string {
	id, ok := ctx.Value(requestIDKey).(string)
	if !ok {
		return ""
	}
	return id
}
