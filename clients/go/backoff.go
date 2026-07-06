package whdrsub

import (
	"math"
	"math/rand"
	"time"
)

// Backoff describes an exponential-backoff-with-jitter reconnect schedule.
//
// Base (pre-jitter) delays follow Initial * Multiplier^attempt, capped at Max,
// then multiplied by a random factor in [1-Jitter, 1+Jitter). A fresh runner
// (see start) begins at attempt 0; the Run loop resets it after every
// successful connection so a long-lived connection that later drops reconnects
// fast.
type Backoff struct {
	// Initial is the delay before the first reconnect attempt.
	Initial time.Duration
	// Max is the upper bound on the pre-jitter delay.
	Max time.Duration
	// Multiplier is the growth factor applied per attempt.
	Multiplier float64
	// Jitter is the jitter fraction in [0.0, 1.0). 0.2 = +/-20%.
	Jitter float64
}

// DefaultBackoff is the schedule used when Config.Backoff is the zero value.
var DefaultBackoff = Backoff{
	Initial:    500 * time.Millisecond,
	Max:        30 * time.Second,
	Multiplier: 2.0,
	Jitter:     0.2,
}

// withDefaults fills any zero fields from DefaultBackoff so a partially-set
// Backoff is still usable.
func (b Backoff) withDefaults() Backoff {
	if b.Initial <= 0 {
		b.Initial = DefaultBackoff.Initial
	}
	if b.Max <= 0 {
		b.Max = DefaultBackoff.Max
	}
	if b.Multiplier < 1.0 {
		b.Multiplier = DefaultBackoff.Multiplier
	}
	if b.Jitter < 0 {
		b.Jitter = 0
	}
	return b
}

// baseDelay is the deterministic (pre-jitter) delay for a given attempt number.
func (b Backoff) baseDelay(attempt int) time.Duration {
	factor := math.Pow(b.Multiplier, float64(attempt))
	millis := float64(b.Initial.Milliseconds()) * factor
	if capMillis := float64(b.Max.Milliseconds()); millis > capMillis {
		millis = capMillis
	}
	return time.Duration(math.Round(millis)) * time.Millisecond
}

// applyJitter maps rand01 in [0,1) to a factor in [1-jitter, 1+jitter) and
// scales base by it. Factored out for deterministic testing.
func applyJitter(base time.Duration, jitter, rand01 float64) time.Duration {
	if jitter <= 0 {
		return base
	}
	factor := 1.0 - jitter + rand01*2.0*jitter
	return time.Duration(float64(base) * factor)
}

// backoffRunner is the running state for a Backoff.
type backoffRunner struct {
	policy  Backoff
	attempt int
	rand    func() float64
}

// start begins a fresh runner at attempt 0.
func (b Backoff) start() *backoffRunner {
	return &backoffRunner{policy: b.withDefaults(), rand: rand.Float64}
}

// reset returns to the initial delay (call after a successful connection).
func (r *backoffRunner) reset() { r.attempt = 0 }

// nextDelay computes the next delay (with jitter) and advances the attempt
// counter.
func (r *backoffRunner) nextDelay() time.Duration {
	base := r.policy.baseDelay(r.attempt)
	if r.attempt < math.MaxInt {
		r.attempt++
	}
	return applyJitter(base, r.policy.Jitter, r.rand())
}
