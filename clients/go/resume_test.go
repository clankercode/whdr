package whdrsub

import (
	"fmt"
	"testing"
)

func id(n int) string { return fmt.Sprintf("id-%d", n) }

// Conformance item 4: ignore frames with seq <= cursor.
func TestSkipsSeqAtOrBelowCursor(t *testing.T) {
	s := newResumeState(0, 16)
	if !s.shouldProcess(id(1), 1) {
		t.Fatal("seq 1 should process from cursor 0")
	}
	s.record(id(1), 1)
	if s.Cursor() != 1 {
		t.Fatalf("cursor = %d, want 1", s.Cursor())
	}
	// A replayed duplicate at seq 1 is now at/below the cursor.
	if s.shouldProcess(id(1), 1) {
		t.Fatal("duplicate seq 1 should be skipped")
	}
	// A brand-new lower seq is also guarded.
	if s.shouldProcess(id(99), 1) {
		t.Fatal("seq 1 below cursor should be skipped regardless of id")
	}
	if !s.shouldProcess(id(2), 2) {
		t.Fatal("seq 2 above cursor should process")
	}
}

// Conformance item 4: dedup by id across the replay/live boundary.
func TestDedupsByIDAcrossReplayLiveBoundary(t *testing.T) {
	s := newResumeState(5, 16)
	if !s.shouldProcess(id(7), 6) {
		t.Fatal("new event should process")
	}
	s.record(id(7), 6)
	if s.shouldProcess(id(7), 6) {
		t.Fatal("same id at same seq should dedup")
	}
	// id in seen set is skipped even at a higher seq label.
	if s.shouldProcess(id(7), 8) {
		t.Fatal("same id at higher seq should still dedup")
	}
}

// Conformance item 5: the cursor advances only after record (i.e. after handle).
func TestCursorAdvancesOnlyViaRecord(t *testing.T) {
	s := newResumeState(10, 16)
	if !s.shouldProcess(id(1), 11) {
		t.Fatal("seq 11 should process")
	}
	if s.Cursor() != 10 {
		t.Fatalf("cursor moved on shouldProcess: %d", s.Cursor())
	}
	s.record(id(1), 11)
	if s.Cursor() != 11 {
		t.Fatalf("cursor = %d, want 11", s.Cursor())
	}
}

func TestBoundedSeenSetEvictsOldest(t *testing.T) {
	s := newResumeState(0, 2)
	s.record(id(1), 1)
	s.record(id(2), 2)
	s.record(id(3), 3) // evicts id(1)
	// id(1) is evicted from the recent-id set, but its seq (1) is below the
	// cursor (now 3), so it is still guarded.
	if s.shouldProcess(id(1), 1) {
		t.Fatal("evicted id below cursor should still be guarded by seq")
	}
	if s.shouldProcess(id(2), 2) {
		t.Fatal("id(2) still remembered by id")
	}
}

func TestCapacityClampedToOne(t *testing.T) {
	s := newResumeState(0, 0)
	if s.capacity != 1 {
		t.Fatalf("capacity = %d, want clamped to 1", s.capacity)
	}
}
