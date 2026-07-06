package whdrsub

import (
	"testing"
	"time"
)

func TestBaseDelaysGrowAndCap(t *testing.T) {
	policy := Backoff{
		Initial:    500 * time.Millisecond,
		Max:        8 * time.Second,
		Multiplier: 2.0,
		Jitter:     0.0,
	}
	r := policy.start()
	want := []time.Duration{
		500 * time.Millisecond,
		1000 * time.Millisecond,
		2000 * time.Millisecond,
		4000 * time.Millisecond,
		8000 * time.Millisecond, // cap
		8000 * time.Millisecond, // cap
	}
	for i, w := range want {
		if got := r.nextDelay(); got != w {
			t.Fatalf("attempt %d: got %v, want %v", i, got, w)
		}
	}
}

func TestResetReturnsToInitial(t *testing.T) {
	r := Backoff{
		Initial:    500 * time.Millisecond,
		Max:        30 * time.Second,
		Multiplier: 2.0,
		Jitter:     0.0,
	}.start()
	first := r.nextDelay()
	r.nextDelay()
	r.nextDelay()
	r.reset()
	if got := r.nextDelay(); got != first {
		t.Fatalf("after reset got %v, want %v", got, first)
	}
}

func TestJitterStaysWithinBounds(t *testing.T) {
	base := time.Second
	lo := applyJitter(base, 0.2, 0.0)
	hi := applyJitter(base, 0.2, 0.9999)
	if lo < 800*time.Millisecond || lo > base {
		t.Fatalf("lo jitter out of bounds: %v", lo)
	}
	if hi < base || hi >= 1200*time.Millisecond {
		t.Fatalf("hi jitter out of bounds: %v", hi)
	}
	if got := applyJitter(base, 0.0, 0.5); got != base {
		t.Fatalf("zero jitter should be exact: %v", got)
	}
}

// A zero-value Backoff falls back to DefaultBackoff.
func TestZeroBackoffUsesDefaults(t *testing.T) {
	r := (Backoff{}).start()
	if r.policy.Initial != DefaultBackoff.Initial ||
		r.policy.Max != DefaultBackoff.Max ||
		r.policy.Multiplier != DefaultBackoff.Multiplier {
		t.Fatalf("zero backoff did not adopt defaults: %+v", r.policy)
	}
}

func TestNextDelayAdvancesWithDeterministicRand(t *testing.T) {
	r := Backoff{
		Initial:    100 * time.Millisecond,
		Max:        10 * time.Second,
		Multiplier: 2.0,
		Jitter:     0.5,
	}.start()
	r.rand = func() float64 { return 0.5 } // factor 1.0
	if got := r.nextDelay(); got != 100*time.Millisecond {
		t.Fatalf("attempt 0 with mid-jitter: got %v, want 100ms", got)
	}
	if got := r.nextDelay(); got != 200*time.Millisecond {
		t.Fatalf("attempt 1 with mid-jitter: got %v, want 200ms", got)
	}
}
