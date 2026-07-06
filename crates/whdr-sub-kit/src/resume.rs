//! Cursor + dedup state implementing the appendix's at-least-once guard.

use std::collections::{HashSet, VecDeque};

use uuid::Uuid;

/// Tracks the resume cursor and a bounded set of recently-seen event `id`s.
///
/// This is the heart of the conformance checklist's dedup rule (items 4 & 5):
/// an event is processed at most once, and the cursor advances only *after* a
/// successful handle. `seq` is a **global** monotonic counter, so gaps in the
/// `seq` values a connection observes are normal (they belong to other
/// subscribers' patterns) — never infer loss from a gap.
#[derive(Debug)]
pub struct ResumeState {
    cursor: u64,
    seen: HashSet<Uuid>,
    order: VecDeque<Uuid>,
    capacity: usize,
}

impl ResumeState {
    /// Create state resuming from `cursor`, remembering up to `capacity`
    /// recent ids for boundary dedup.
    pub fn new(cursor: u64, capacity: usize) -> Self {
        Self {
            cursor,
            seen: HashSet::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    /// The highest `seq` successfully processed so far — the value to send as
    /// `replay.after_seq` on the next (re)connect.
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Whether an event with this `id`/`seq` should be handed to the handler.
    ///
    /// Skips duplicates that appear around the replay/live boundary: a `seq`
    /// at or below the cursor, or an `id` already processed within the recent
    /// window.
    pub fn should_process(&self, id: Uuid, seq: u64) -> bool {
        seq > self.cursor && !self.seen.contains(&id)
    }

    /// Record a successfully-handled event: remember its `id` (evicting the
    /// oldest beyond `capacity`) and advance the cursor.
    pub fn record(&mut self, id: Uuid, seq: u64) {
        if self.seen.insert(id) {
            self.order.push_back(id);
            if self.order.len() > self.capacity
                && let Some(old) = self.order.pop_front()
            {
                self.seen.remove(&old);
            }
        }
        if seq > self.cursor {
            self.cursor = seq;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    #[test]
    fn skips_seq_at_or_below_cursor() {
        let mut state = ResumeState::new(0, 16);
        assert!(state.should_process(id(1), 1));
        state.record(id(1), 1);
        assert_eq!(state.cursor(), 1);
        // A replayed duplicate at seq 1 is now below the cursor.
        assert!(!state.should_process(id(1), 1));
        // A brand-new lower seq (shouldn't happen live, but guard holds).
        assert!(!state.should_process(id(99), 1));
        // Next higher seq proceeds.
        assert!(state.should_process(id(2), 2));
    }

    #[test]
    fn dedups_by_id_across_replay_live_boundary() {
        let mut state = ResumeState::new(5, 16);
        // Same id delivered once via replay, once live: process exactly once.
        assert!(state.should_process(id(7), 6));
        state.record(id(7), 6);
        assert!(!state.should_process(id(7), 6), "duplicate id skipped");
        // id(7) is in `seen`, so it is skipped even at a higher seq label.
        assert!(!state.should_process(id(7), 8));
    }

    #[test]
    fn cursor_advances_only_via_record() {
        let mut state = ResumeState::new(10, 16);
        // Merely asking does not move the cursor.
        assert!(state.should_process(id(1), 11));
        assert_eq!(state.cursor(), 10);
        state.record(id(1), 11);
        assert_eq!(state.cursor(), 11);
    }

    #[test]
    fn bounded_seen_set_evicts_oldest() {
        let mut state = ResumeState::new(0, 2);
        state.record(id(1), 1);
        state.record(id(2), 2);
        state.record(id(3), 3); // evicts id(1)
        // id(1) is evicted, but its seq (1) is below the cursor (now 3),
        // so it is still guarded from reprocessing.
        assert!(!state.should_process(id(1), 1));
        // id(2) still remembered by id.
        assert!(!state.should_process(id(2), 2));
    }
}
