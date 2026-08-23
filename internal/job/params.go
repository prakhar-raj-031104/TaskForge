package job

import (
	"encoding/json"
	"regexp"
	"strings"
	"time"
)

// typePattern constrains job types to a lowercase, machine-friendly shape.
//
// Compiled once at package init: regexp.MustCompile inside a request handler
// would recompile the pattern on every request, and MustCompile is the correct
// choice here because a malformed literal is a programming error that should
// stop the process at startup, not a runtime condition to handle.
var typePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,99}$`)

// CreateParams is a validated job-creation request.
//
// Optional fields are pointers so the service can tell "the client omitted
// this, use the default" from "the client explicitly sent zero". With a plain
// int, priority 0 and priority-not-supplied are the same value, and the client
// silently gets 50 when it asked for something invalid.
type CreateParams struct {
	Type           string
	Payload        json.RawMessage
	Priority       *int
	MaxRetries     *int
	ScheduledAt    *time.Time
	IdempotencyKey *string
}

// Validate checks every field and reports all problems at once.
//
// now is passed in rather than read from time.Now() so that scheduling rules
// are testable without sleeping or monkey-patching the clock.
func (p CreateParams) Validate(now time.Time) error {
	var v ValidationError

	switch {
	case strings.TrimSpace(p.Type) == "":
		v.Add("type", "is required")
	case len(p.Type) > MaxTypeLength:
		v.Add("type", "must be at most %d characters, got %d", MaxTypeLength, len(p.Type))
	case !typePattern.MatchString(p.Type):
		v.Add("type", "must start with a lowercase letter and contain only lowercase letters, digits, underscores, dots or hyphens")
	}

	if len(p.Payload) > 0 {
		// json.Valid catches malformed JSON. We additionally require an object
		// because handlers address payloads by key; accepting a bare array or
		// scalar would push that type check into every handler.
		if !json.Valid(p.Payload) {
			v.Add("payload", "must be valid JSON")
		} else if trimmed := strings.TrimSpace(string(p.Payload)); !strings.HasPrefix(trimmed, "{") {
			v.Add("payload", "must be a JSON object")
		}
	}

	if p.Priority != nil && (*p.Priority < MinPriority || *p.Priority > MaxPriority) {
		v.Add("priority", "must be between %d and %d, got %d", MinPriority, MaxPriority, *p.Priority)
	}

	if p.MaxRetries != nil && (*p.MaxRetries < 0 || *p.MaxRetries > MaxMaxRetries) {
		v.Add("max_retries", "must be between 0 and %d, got %d", MaxMaxRetries, *p.MaxRetries)
	}

	if p.ScheduledAt != nil {
		switch {
		case p.ScheduledAt.IsZero():
			v.Add("scheduled_at", "must be a valid RFC 3339 timestamp")
		case p.ScheduledAt.After(now.Add(MaxScheduleAhead)):
			// A job scheduled for the year 3026 is a typo, not a requirement.
			v.Add("scheduled_at", "must be at most %s in the future", MaxScheduleAhead)
		}
	}

	if p.IdempotencyKey != nil {
		key := strings.TrimSpace(*p.IdempotencyKey)
		switch {
		case key == "":
			v.Add("idempotency_key", "must not be blank when supplied")
		case len(key) > MaxIdempotencyKeyLength:
			v.Add("idempotency_key", "must be at most %d characters, got %d", MaxIdempotencyKeyLength, len(key))
		}
	}

	return v.ErrOrNil()
}

// ListFilter narrows a job listing.
type ListFilter struct {
	Statuses []Status
	Type     string
	Limit    int
	Offset   int
}

// Listing limits.
const (
	DefaultListLimit = 50
	MaxListLimit     = 200
	// MaxListOffset caps how deep a client may page.
	//
	// OFFSET makes PostgreSQL walk and discard every skipped row, so page 10000
	// costs 10000x page 1. The cap keeps a pathological request from pinning a
	// connection. Deep pagination should move to keyset ("WHERE (created_at, id)
	// < (?, ?)") if a real use case ever appears; until then a cap is cheaper
	// than the machinery.
	MaxListOffset = 10000
)

// Validate checks the filter and reports all problems at once.
func (f ListFilter) Validate() error {
	var v ValidationError

	for _, s := range f.Statuses {
		if !s.Valid() {
			v.Add("status", "unknown status %q", s)
		}
	}
	if f.Type != "" && !typePattern.MatchString(f.Type) {
		v.Add("type", "is not a valid job type")
	}
	if f.Limit < 0 || f.Limit > MaxListLimit {
		v.Add("limit", "must be between 1 and %d, got %d", MaxListLimit, f.Limit)
	}
	if f.Offset < 0 || f.Offset > MaxListOffset {
		v.Add("offset", "must be between 0 and %d, got %d", MaxListOffset, f.Offset)
	}

	return v.ErrOrNil()
}

// Normalize applies defaults. Called after Validate.
func (f ListFilter) Normalize() ListFilter {
	if f.Limit <= 0 {
		f.Limit = DefaultListLimit
	}
	return f
}
