package job

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestRegistryRegisterAndGet(t *testing.T) {
	t.Parallel()

	r := NewRegistry()

	called := false
	h := HandlerFunc(func(context.Context, *Job) error {
		called = true
		return nil
	})

	if err := r.Register("send_email", h); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, err := r.Get("send_email")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if err := got.Handle(context.Background(), &Job{}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !called {
		t.Error("the registered function was not the one invoked")
	}
}

// TestRegistryRejectsDuplicates: two packages both claiming "send_email" is a
// bug that must surface at startup, not as jobs silently running the wrong code.
func TestRegistryRejectsDuplicates(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	h := HandlerFunc(func(context.Context, *Job) error { return nil })

	if err := r.Register("send_email", h); err != nil {
		t.Fatalf("first Register: %v", err)
	}

	err := r.Register("send_email", h)
	if err == nil {
		t.Fatal("the duplicate registration was accepted")
	}
	if !strings.Contains(err.Error(), "already registered") {
		t.Errorf("error = %q, want it to mention the duplicate", err)
	}
}

func TestRegistryRejectsInvalidRegistrations(t *testing.T) {
	t.Parallel()

	r := NewRegistry()

	if err := r.Register("", HandlerFunc(func(context.Context, *Job) error { return nil })); err == nil {
		t.Error("an empty job type was accepted")
	}
	if err := r.Register("valid", nil); err == nil {
		t.Error("a nil handler was accepted")
	}
}

// TestRegistryUnknownTypeIsPermanent: no amount of retrying will conjure a
// handler into existence, so such a job must dead-letter on the first attempt.
func TestRegistryUnknownTypeIsPermanent(t *testing.T) {
	t.Parallel()

	r := NewRegistry()

	_, err := r.Get("never_registered")
	if err == nil {
		t.Fatal("Get returned no error for an unregistered type")
	}
	if !errors.Is(err, ErrUnknownJobType) {
		t.Errorf("errors.Is(err, ErrUnknownJobType) = false for %v", err)
	}
	if !IsPermanent(err) {
		t.Error("an unknown job type must be a permanent failure")
	}
	if !strings.Contains(err.Error(), "never_registered") {
		t.Errorf("error %q does not name the type", err)
	}
}

func TestRegistryTypesIsSorted(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	h := HandlerFunc(func(context.Context, *Job) error { return nil })

	for _, name := range []string{"webhook", "sleep", "send_email", "flaky"} {
		if err := r.Register(name, h); err != nil {
			t.Fatal(err)
		}
	}

	want := []string{"flaky", "send_email", "sleep", "webhook"}
	got := r.Types()
	if len(got) != len(want) {
		t.Fatalf("Types() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Types()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestRegistryConcurrentReads exists to be run under -race. Every worker
// goroutine calls Get on every job, so an unsynchronised map here would be a
// data race in the hottest path in the system.
func TestRegistryConcurrentReads(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	for i := range 8 {
		if err := r.Register(fmt.Sprintf("type_%d", i),
			HandlerFunc(func(context.Context, *Job) error { return nil })); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for range 50 {
				if _, err := r.Get(fmt.Sprintf("type_%d", n%8)); err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				_ = r.Types()
				_ = r.Has("type_1")
			}
		}(i)
	}
	wg.Wait()
}

func TestPermanentError(t *testing.T) {
	t.Parallel()

	if Permanent(nil) != nil {
		t.Error("Permanent(nil) must return nil so it can be used inline")
	}

	cause := errors.New("malformed payload")
	err := Permanent(cause)

	if !IsPermanent(err) {
		t.Error("IsPermanent = false for a permanent error")
	}
	// Unwrap keeps the cause reachable, so a caller can still ask what actually
	// went wrong rather than only that it was permanent.
	if !errors.Is(err, cause) {
		t.Error("errors.Is could not find the cause through the wrapper")
	}
	if !strings.Contains(err.Error(), "malformed payload") {
		t.Errorf("error = %q, want it to include the cause", err)
	}

	if IsPermanent(errors.New("transient")) {
		t.Error("a plain error was reported as permanent")
	}

	// Permanence survives further wrapping, which matters because handlers wrap
	// their errors on the way out.
	wrapped := fmt.Errorf("calling provider: %w", err)
	if !IsPermanent(wrapped) {
		t.Error("permanence was lost when the error was wrapped again")
	}
}
