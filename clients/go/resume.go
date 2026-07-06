package whdrsub

import "container/list"

// resumeState tracks the resume cursor and a bounded set of recently-seen event
// ids. It is the heart of the conformance checklist's dedup rule (items 4 & 5):
// an event is processed at most once, and the cursor advances only after a
// successful handle.
//
// seq is a global monotonic counter, so gaps in the seq values a connection
// observes are normal (they belong to other subscribers' patterns) — never
// infer loss from a gap.
//
// resumeState is not safe for concurrent use; the Run loop drives it from a
// single goroutine.
type resumeState struct {
	cursor   uint64
	seen     map[string]*list.Element
	order    *list.List // front = oldest id, back = newest
	capacity int
}

// newResumeState creates state resuming from cursor, remembering up to capacity
// recent ids for boundary dedup. A capacity below 1 is clamped to 1.
func newResumeState(cursor uint64, capacity int) *resumeState {
	if capacity < 1 {
		capacity = 1
	}
	return &resumeState{
		cursor:   cursor,
		seen:     make(map[string]*list.Element),
		order:    list.New(),
		capacity: capacity,
	}
}

// Cursor returns the highest seq successfully processed so far — the value to
// send as replay.after_seq on the next (re)connect.
func (r *resumeState) Cursor() uint64 { return r.cursor }

// shouldProcess reports whether an event with this id/seq should be handed to
// the handler. It skips duplicates that appear around the replay/live boundary:
// a seq at or below the cursor, or an id already processed within the recent
// window.
func (r *resumeState) shouldProcess(id string, seq uint64) bool {
	if seq <= r.cursor {
		return false
	}
	_, dup := r.seen[id]
	return !dup
}

// record marks a successfully-handled event: it remembers the id (evicting the
// oldest beyond capacity) and advances the cursor.
func (r *resumeState) record(id string, seq uint64) {
	if _, dup := r.seen[id]; !dup {
		elem := r.order.PushBack(id)
		r.seen[id] = elem
		if r.order.Len() > r.capacity {
			oldest := r.order.Front()
			if oldest != nil {
				r.order.Remove(oldest)
				delete(r.seen, oldest.Value.(string))
			}
		}
	}
	if seq > r.cursor {
		r.cursor = seq
	}
}
