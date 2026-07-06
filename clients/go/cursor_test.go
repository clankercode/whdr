package whdrsub

import (
	"path/filepath"
	"testing"
)

func TestMemoryStoreRoundTrips(t *testing.T) {
	s := NewMemoryCursorStore(42)
	if got, _ := s.Load(); got != 42 {
		t.Fatalf("load = %d, want 42", got)
	}
	if err := s.Save(100); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Load(); got != 100 {
		t.Fatalf("load = %d, want 100", got)
	}
	if s.Get() != 100 {
		t.Fatalf("get = %d, want 100", s.Get())
	}
}

func TestFileStoreRoundTripsAndAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor")
	s := NewFileCursorStore(path)

	// Missing file reads as 0.
	if got, err := s.Load(); err != nil || got != 0 {
		t.Fatalf("missing file: got %d, err %v", got, err)
	}
	if err := s.Save(777); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Load(); err != nil || got != 777 {
		t.Fatalf("after save: got %d, err %v", got, err)
	}
	// A fresh store over the same path sees the persisted value (cross-restart).
	if got, _ := NewFileCursorStore(path).Load(); got != 777 {
		t.Fatalf("reopened store: got %d, want 777", got)
	}
}
