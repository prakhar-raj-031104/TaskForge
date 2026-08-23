package job

import (
	"testing"
	"time"
)

// noJitter makes the jitter multiplier deterministic so the exponential curve
// itself can be asserted. rand() == 0 yields the lower bound of the window.
func noJitter() float64 { return 0 }

// maxJitter yields the upper bound.
func maxJitter() float64 { return 0.9999999 }

func TestBackoffGrowsExponentially(t *testing.T) {
	t.Parallel()

	b := Backoff{Base: time.Second, Max: time.Hour, Factor: 2}.WithRand(noJitter)

	// With rand()==0 the delay is exactly half the computed value, because
	// equal jitter is half + random(0, half).
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 500 * time.Millisecond}, // 1s  / 2
		{2, 1 * time.Second},        // 2s  / 2
		{3, 2 * time.Second},        // 4s  / 2
		{4, 4 * time.Second},        // 8s  / 2
		{5, 8 * time.Second},        // 16s / 2
		{10, 256 * time.Second},     // 512s / 2
	}

	for _, tt := range tests {
		if got := b.Delay(tt.attempt); got != tt.want {
			t.Errorf("Delay(%d) = %s, want %s", tt.attempt, got, tt.want)
		}
	}
}

func TestBackoffRespectsTheCap(t *testing.T) {
	t.Parallel()

	b := Backoff{Base: time.Second, Max: 10 * time.Second, Factor: 2}.WithRand(maxJitter)

	for attempt := 1; attempt <= 50; attempt++ {
		if got := b.Delay(attempt); got > 10*time.Second {
			t.Fatalf("Delay(%d) = %s, which exceeds the 10s cap", attempt, got)
		}
	}
}

// TestBackoffDoesNotOverflow is the regression test for the arithmetic trap:
// base * 2^attempt overflows int64 nanoseconds long before attempt 1000, and an
// overflowed Duration goes NEGATIVE, which would schedule the retry in the past
// and spin the queue at full speed.
func TestBackoffDoesNotOverflow(t *testing.T) {
	t.Parallel()

	b := Backoff{Base: time.Hour, Max: 5 * time.Minute, Factor: 10}.WithRand(maxJitter)

	for _, attempt := range []int{100, 1000, 100000} {
		got := b.Delay(attempt)
		if got < 0 {
			t.Errorf("Delay(%d) = %s, a negative delay", attempt, got)
		}
		if got > 5*time.Minute {
			t.Errorf("Delay(%d) = %s, above the cap", attempt, got)
		}
	}
}

// TestBackoffJitterStaysInTheEqualJitterWindow states the contract: never below
// half the computed delay (so retries keep a guaranteed minimum spacing), never
// above it (so the cap holds).
func TestBackoffJitterStaysInTheEqualJitterWindow(t *testing.T) {
	t.Parallel()

	b := Backoff{Base: time.Second, Max: time.Hour, Factor: 2}

	for attempt := 1; attempt <= 8; attempt++ {
		expected := time.Duration(float64(time.Second) * pow(2, attempt-1))
		lower, upper := expected/2, expected

		for range 200 {
			got := b.Delay(attempt)
			if got < lower || got > upper {
				t.Fatalf("Delay(%d) = %s, outside the window [%s, %s]", attempt, got, lower, upper)
			}
		}
	}
}

// TestBackoffJitterActuallyVaries is the point of the whole exercise: if every
// worker computed the same delay, 5,000 jobs failing together would retry
// together and flatten the recovering dependency.
func TestBackoffJitterActuallyVaries(t *testing.T) {
	t.Parallel()

	b := Backoff{Base: time.Second, Max: time.Hour, Factor: 2}

	seen := make(map[time.Duration]struct{})
	for range 100 {
		seen[b.Delay(3)] = struct{}{}
	}

	// 100 draws from a continuous window should not collapse to a handful of
	// values. Anything under 50 distinct results means the jitter is broken.
	if len(seen) < 50 {
		t.Errorf("100 calls produced only %d distinct delays; jitter is not working", len(seen))
	}
}

func TestBackoffHandlesDegenerateInput(t *testing.T) {
	t.Parallel()

	// A zero-value Backoff must still produce something sane rather than 0,
	// which would retry instantly in a tight loop.
	var zero Backoff
	if got := zero.Delay(1); got <= 0 {
		t.Errorf("zero-value Backoff produced %s, want a positive delay", got)
	}

	b := Backoff{Base: time.Second, Max: time.Minute, Factor: 2}.WithRand(noJitter)
	// Attempt 0 and negative attempts are clamped to the first step rather than
	// producing a negative exponent.
	if b.Delay(0) != b.Delay(1) {
		t.Errorf("Delay(0) = %s, want it clamped to Delay(1) = %s", b.Delay(0), b.Delay(1))
	}
	if b.Delay(-5) != b.Delay(1) {
		t.Errorf("Delay(-5) = %s, want it clamped to Delay(1)", b.Delay(-5))
	}
}

func pow(base float64, exp int) float64 {
	result := 1.0
	for range exp {
		result *= base
	}
	return result
}
