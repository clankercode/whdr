package whdrsub

import (
	"os"
	"strconv"
	"strings"
	"sync"
)

// CursorStore loads and persists the resume cursor across sessions.
//
// Implement this to make at-least-once delivery survive process restarts: Load
// is called once at Run start, and Save is called after each event is
// successfully handled. If you only need not-missing-while-briefly-disconnected,
// the default in-memory store (MemoryCursorStore) is enough.
//
// A Load/Save error is fatal to the Run loop (wrapped in CursorStoreError): a
// client that cannot persist its cursor cannot honour its at-least-once
// contract.
type CursorStore interface {
	// Load returns the last persisted cursor (0 to replay from the start of
	// retention).
	Load() (uint64, error)
	// Save persists a cursor value. Called after each successfully-handled
	// event, so it should be cheap and idempotent.
	Save(cursor uint64) error
}

// MemoryCursorStore is an in-memory CursorStore seeded from an initial value.
// It is the default when Config.CursorStore is nil; it does not survive process
// restarts. Safe for concurrent use.
type MemoryCursorStore struct {
	mu     sync.Mutex
	cursor uint64
}

// NewMemoryCursorStore creates a store seeded with initial (the resume cursor).
func NewMemoryCursorStore(initial uint64) *MemoryCursorStore {
	return &MemoryCursorStore{cursor: initial}
}

// Load returns the current cursor.
func (m *MemoryCursorStore) Load() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cursor, nil
}

// Save stores the cursor.
func (m *MemoryCursorStore) Save(cursor uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cursor = cursor
	return nil
}

// Get returns the current cursor value (a convenience for callers holding a
// handle to the store).
func (m *MemoryCursorStore) Get() uint64 {
	c, _ := m.Load()
	return c
}

// FileCursorStore persists the cursor as a decimal u64 in a file, for
// at-least-once delivery across process restarts. A missing file reads as
// cursor 0 (replay from the start of retention). Safe for concurrent use.
//
// Save writes atomically (write temp + rename) so a crash mid-write cannot
// corrupt the stored cursor.
type FileCursorStore struct {
	path string
	mu   sync.Mutex
}

// NewFileCursorStore creates a store backed by path.
func NewFileCursorStore(path string) *FileCursorStore {
	return &FileCursorStore{path: path}
}

// Load reads the persisted cursor. A missing file yields 0.
func (f *FileCursorStore) Load() (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := os.ReadFile(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return 0, nil
	}
	return strconv.ParseUint(text, 10, 64)
}

// Save writes the cursor atomically.
func (f *FileCursorStore) Save(cursor uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(cursor, 10)), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
}
