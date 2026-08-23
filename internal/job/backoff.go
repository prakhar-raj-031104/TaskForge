package job

import (
	"math"
	"math/rand/v2"
	"time"
)

// Backoff decides how long to wait before retrying a failed job.
type Backoff struct {
	// Base is the delay before the first retry.
	Base time.Duration
	// Max caps the delay however many attempts have been made. Without a cap,
	// doubling reaches days: attempt 20 at a 1s base is roughly 12 days.
	Max time.Duration
	// Factor is the growth rate per attempt. 2 doubles each time.
	Factor float64

	// randFloat returns a number in [0,1). Injectable so tests can be
	// deterministic; nil means use the global source.
	randFloat func() float64
}

// DefaultBackoff is the policy used unless configuration overrides it:
// 1s, 2s, 4s, 8s... capped at 5 minutes, each value then jittered.
var DefaultBackoff = Backoff{
	Base:   time.Second,
	Max:    5 * time.Minute,
	Factor: 2,
}

// WithRand returns a copy of b using the supplied random source.
func (b Backoff) WithRand(randFloat func() float64) Backoff {
	b.randFloat = randFloat
	return b
}

// Delay returns how long to wait before the next attempt.
//
// attempt is the number of attempts already made, so the first retry is
// attempt 1.
//
// # Why jitter
//
// Consider a downstream API that goes down for 30 seconds while 5,000 jobs are
// in flight. All 5,000 fail at roughly the same instant. With pure exponential
// backoff every one of them retries at exactly base, then exactly 2*base, then
// exactly 4*base — 5,000 simultaneous requests, in lockstep, forever. The
// retries themselves become the outage: the API comes back up, is immediately
// flattened by a synchronised thundering herd, and falls over again.
//
// Jitter spreads those retries across a window so the load arrives smoothly.
// It is not an optimisation; without it, backoff makes a recovering dependency
// worse rather than better.
//
// This implementation uses "equal jitter": half the computed delay, plus a
// random amount up to the other half. The alternative, "full jitter"
// (random(0, delay)), spreads slightly better but can produce a near-zero wait,
// which is the wrong behaviour when the dependency is still down. Equal jitter
// keeps a guaranteed minimum spacing while still breaking up the herd.
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	base := b.Base
	if base <= 0 {
		base = DefaultBackoff.Base
	}
	maximum := b.Max
	if maximum <= 0 {
		maximum = DefaultBackoff.Max
	}
	factor := b.Factor
	if factor <= 1 {
		factor = DefaultBackoff.Factor
	}

	// math.Pow on a float, then compare against the cap BEFORE converting back
	// to a Duration. A large attempt count overflows int64 nanoseconds
	// (~292 years), and an overflowed Duration goes negative — which would
	// schedule the retry in the past and spin the queue.
	growth := math.Pow(factor, float64(attempt-1))
	nanos := float64(base) * growth

	if math.IsInf(nanos, 0) || nanos > float64(maximum) {
		nanos = float64(maximum)
	}

	half := nanos / 2
	delay := half + half*b.random()

	if delay > float64(maximum) {
		delay = float64(maximum)
	}
	return time.Duration(delay)
}

func (b Backoff) random() float64 {
	if b.randFloat != nil {
		return b.randFloat()
	}
	// math/rand/v2's top-level functions are safe for concurrent use and are
	// seeded randomly at startup, so several worker processes do not share a
	// retry schedule.
	return rand.Float64()
}
