package job

import (
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors. Callers compare with errors.Is rather than string matching,
// so the message can change without breaking anyone.
var (
	// ErrNotFound means no job matched the identifier.
	ErrNotFound = errors.New("job not found")

	// ErrInvalidTransition means a caller attempted an illegal state move.
	ErrInvalidTransition = errors.New("invalid job status transition")

	// ErrLeaseLost means the worker's fencing epoch no longer matches the row:
	// its lease expired and someone else owns the job now. The worker must
	// abandon its result rather than overwrite the new owner's.
	ErrLeaseLost = errors.New("job lease lost")
)

// FieldError is a single problem with one input field.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError collects every problem with a request instead of stopping at
// the first. A client fixing three fields should need one round trip, not three.
type ValidationError struct {
	Fields []FieldError
}

// Add records a problem with a field.
func (e *ValidationError) Add(field, format string, args ...any) {
	e.Fields = append(e.Fields, FieldError{
		Field:   field,
		Message: fmt.Sprintf(format, args...),
	})
}

// HasErrors reports whether anything was recorded.
func (e *ValidationError) HasErrors() bool { return len(e.Fields) > 0 }

// ErrOrNil returns e when it holds problems and a nil error otherwise.
//
// This exists because of a genuine Go trap: returning a typed nil pointer as an
// error interface produces a non-nil interface value, so `if err != nil` fires
// on what the author believed was success. Funnelling every return through this
// helper means the trap cannot be hit.
func (e *ValidationError) ErrOrNil() error {
	if e == nil || !e.HasErrors() {
		return nil
	}
	return e
}

func (e *ValidationError) Error() string {
	parts := make([]string, 0, len(e.Fields))
	for _, f := range e.Fields {
		parts = append(parts, f.Field+": "+f.Message)
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

// InvalidTransitionError says which move was rejected, so the log line is
// actionable rather than just "invalid transition".
type InvalidTransitionError struct {
	From Status
	To   Status
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("cannot transition job from %s to %s", e.From, e.To)
}

// Is makes errors.Is(err, ErrInvalidTransition) succeed for this type, so
// callers can either match the sentinel broadly or use errors.As to read the
// From/To detail.
func (e *InvalidTransitionError) Is(target error) bool {
	return target == ErrInvalidTransition
}
