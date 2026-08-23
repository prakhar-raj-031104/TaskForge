package job

import (
	"strings"
	"testing"
	"time"
)

func TestStatusValid(t *testing.T) {
	t.Parallel()

	for _, s := range Statuses() {
		if !s.Valid() {
			t.Errorf("Statuses() returned %q but Valid() rejects it", s)
		}
	}
	for _, s := range []Status{"", "PENDING", "failed", "retrying", "done"} {
		if s.Valid() {
			t.Errorf("Valid() accepted unknown status %q", s)
		}
	}
}

// TestStatusesReturnsACopy guards against a caller mutating the package's own
// slice through the returned header.
func TestStatusesReturnsACopy(t *testing.T) {
	t.Parallel()

	got := Statuses()
	got[0] = "tampered"

	if Statuses()[0] != StatusPending {
		t.Fatal("mutating the result of Statuses() changed the package state")
	}
}

func TestStatusIsTerminal(t *testing.T) {
	t.Parallel()

	terminal := map[Status]bool{
		StatusPending:    false,
		StatusRunning:    false,
		StatusCompleted:  true,
		StatusDeadLetter: true,
		StatusCancelled:  true,
	}

	for s, want := range terminal {
		if got := s.IsTerminal(); got != want {
			t.Errorf("%s.IsTerminal() = %v, want %v", s, got, want)
		}
	}
}

// TestStateMachine is the specification of the job lifecycle, written as a
// table. Every legal edge is listed, and everything not listed must be refused.
func TestStateMachine(t *testing.T) {
	t.Parallel()

	legal := map[Status][]Status{
		StatusPending:    {StatusRunning, StatusCancelled},
		StatusRunning:    {StatusCompleted, StatusPending, StatusDeadLetter, StatusCancelled},
		StatusCompleted:  {},
		StatusDeadLetter: {StatusPending},
		StatusCancelled:  {},
	}

	for from, allowed := range legal {
		for _, to := range Statuses() {
			want := false
			for _, a := range allowed {
				if a == to {
					want = true
					break
				}
			}

			if got := from.CanTransitionTo(to); got != want {
				t.Errorf("%s -> %s: CanTransitionTo = %v, want %v", from, to, got, want)
			}
		}
	}
}

// TestTerminalStatesAreSinks states the invariant plainly: once terminal,
// nothing except an explicit dead-letter replay moves a job again.
func TestTerminalStatesAreSinks(t *testing.T) {
	t.Parallel()

	for _, from := range []Status{StatusCompleted, StatusCancelled} {
		for _, to := range Statuses() {
			if from.CanTransitionTo(to) {
				t.Errorf("%s is terminal but allows a transition to %s", from, to)
			}
		}
	}

	// dead_letter is the one deliberate exception: an operator can replay it.
	if !StatusDeadLetter.CanTransitionTo(StatusPending) {
		t.Error("dead_letter must be replayable to pending")
	}
	if StatusDeadLetter.CanTransitionTo(StatusCompleted) {
		t.Error("dead_letter must not jump straight to completed")
	}
}

func TestRetriesRemaining(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attempts, maxRetries, want int
	}{
		{0, 3, 3},
		{1, 3, 2},
		{3, 3, 0},
		{5, 3, 0}, // never negative
		{0, 0, 0},
	}

	for _, tt := range tests {
		j := &Job{Attempts: tt.attempts, MaxRetries: tt.maxRetries}
		if got := j.RetriesRemaining(); got != tt.want {
			t.Errorf("attempts=%d max=%d: RetriesRemaining = %d, want %d",
				tt.attempts, tt.maxRetries, got, tt.want)
		}
	}
}

func TestLeaseExpired(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Second)
	future := now.Add(time.Second)

	tests := []struct {
		name  string
		lease *time.Time
		want  bool
	}{
		{"no lease held", nil, false},
		{"lease still valid", &future, false},
		{"lease elapsed", &past, true},
		{"lease expiring exactly now", &now, false}, // After is strict
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{LeaseUntil: tt.lease}
			if got := j.LeaseExpired(now); got != tt.want {
				t.Errorf("LeaseExpired = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTruncateError(t *testing.T) {
	t.Parallel()

	short := "connection refused"
	if got := TruncateError(short); got != short {
		t.Errorf("a short message was altered: %q", got)
	}

	long := strings.Repeat("x", MaxLastErrorLength+500)
	got := TruncateError(long)
	if len(got) != MaxLastErrorLength {
		t.Errorf("truncated length = %d, want %d", len(got), MaxLastErrorLength)
	}
	if !strings.HasSuffix(got, "...") {
		t.Error("a truncated message should end with an ellipsis")
	}
}

// TestValidationErrorOrNil covers the typed-nil trap described in errors.go.
func TestValidationErrorOrNil(t *testing.T) {
	t.Parallel()

	var empty ValidationError
	if err := empty.ErrOrNil(); err != nil {
		t.Errorf("an empty ValidationError produced a non-nil error: %v", err)
	}

	var filled ValidationError
	filled.Add("priority", "must be between %d and %d", 1, 100)
	err := filled.ErrOrNil()
	if err == nil {
		t.Fatal("a populated ValidationError produced a nil error")
	}
	if !strings.Contains(err.Error(), "priority") {
		t.Errorf("error text %q does not name the field", err)
	}
	if !strings.Contains(err.Error(), "between 1 and 100") {
		t.Errorf("error text %q lost the formatted arguments", err)
	}
}
