package job

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
)

// Handler executes one job type.
//
// The interface is a single method on purpose. A one-method interface can be
// satisfied by a function (see HandlerFunc), is trivial to fake in a test, and
// gives the worker core exactly what it needs and nothing more. This is what
// lets a new job type be added without touching a line of worker code.
//
// Implementations must:
//   - respect ctx, and return promptly when it is cancelled
//   - be idempotent where they can, because delivery is at-least-once
//   - return Permanent(err) for failures that retrying cannot fix
type Handler interface {
	Handle(ctx context.Context, j *Job) error
}

// HandlerFunc adapts a plain function to Handler.
//
// This is the same trick net/http uses for HandlerFunc: a named function type
// with a method on it satisfies the interface, so simple handlers need no
// struct.
type HandlerFunc func(ctx context.Context, j *Job) error

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, j *Job) error { return f(ctx, j) }

// ErrUnknownJobType means no handler is registered for a job's type.
var ErrUnknownJobType = errors.New("no handler registered for job type")

// PermanentError marks a failure that retrying will never fix: a malformed
// payload, a 400 from an upstream API, an unknown job type.
//
// Distinguishing permanent from transient failures is what stops the queue
// burning its entire retry budget - and the associated log volume and upstream
// load - on work that was doomed from the first attempt.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }

// Unwrap lets errors.Is and errors.As see through to the cause, so a caller can
// still ask "was this a context deadline?" about a permanent error.
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent marks err as non-retryable. It returns nil for a nil error so it
// can be used inline without a preceding nil check.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// IsPermanent reports whether err is marked non-retryable anywhere in its chain.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// Registry maps job types to handlers.
//
// This is the extension point of the whole system: the worker core looks up a
// handler by type and calls it. Adding a job type means registering a handler
// in main. The alternative - a switch statement in the worker loop - would mean
// every new job type edits the most safety-critical file in the project.
type Registry struct {
	// In practice the map is written only during startup and read-only
	// afterwards, but "in practice" is not a memory model. The mutex makes the
	// happens-before relationship explicit, and costs nothing measurable next
	// to a database round trip.
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register associates a handler with a job type.
//
// It returns an error on a duplicate registration rather than silently
// overwriting: two packages both claiming "send_email" is a bug that should
// surface at startup, not as jobs mysteriously running the wrong code.
func (r *Registry) Register(jobType string, h Handler) error {
	if jobType == "" {
		return errors.New("job type must not be empty")
	}
	if h == nil {
		return fmt.Errorf("handler for %q must not be nil", jobType)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[jobType]; exists {
		return fmt.Errorf("handler for %q is already registered", jobType)
	}
	r.handlers[jobType] = h
	return nil
}

// Get returns the handler for a job type.
func (r *Registry) Get(jobType string) (Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	h, ok := r.handlers[jobType]
	if !ok {
		// Permanent: no amount of retrying will conjure a handler into
		// existence, so the job goes straight to the dead-letter queue where an
		// operator will see it.
		return nil, Permanent(fmt.Errorf("%w: %q", ErrUnknownJobType, jobType))
	}
	return h, nil
}

// Types returns the registered job types, sorted, for logging at startup.
func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

// Has reports whether a job type has a handler.
func (r *Registry) Has(jobType string) bool {
	return slices.Contains(r.Types(), jobType)
}
