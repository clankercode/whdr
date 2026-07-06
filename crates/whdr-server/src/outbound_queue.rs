//! Per-subscriber outbound frame queue.
//!
//! Replaces a plain bounded mpsc so the queue can enforce a byte budget in
//! addition to a frame count, and evict the *oldest* event on overflow
//! (webhook consumers care about freshness more than completeness). Frames
//! are serialized once at push time and shared as `Arc<str>` across every
//! subscriber's queue, so a fanned-out event costs one allocation total, not
//! one per subscriber.
//!
//! Control frames (ok/error/pong/closing) never count against the budget and
//! are never evicted: they are tiny and bounded by the request/response
//! nature of the subscriber protocol.

use std::collections::VecDeque;
use std::pin::pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

#[derive(Clone)]
pub(crate) struct Frame {
    pub(crate) text: std::sync::Arc<str>,
    pub(crate) closing: bool,
    is_event: bool,
}

pub(crate) struct OutboundQueue {
    inner: Mutex<VecDeque<Frame>>,
    notify: Notify,
    max_events: usize,
    max_event_bytes: usize,
    delivered: AtomicUsize,
    dropped: AtomicUsize,
    /// Drops not yet reported to the connection as a `lagged` frame. Coalesced
    /// into a single frame per burst; cleared when taken (§9, [D-lag]).
    lag_pending: AtomicUsize,
}

impl OutboundQueue {
    pub(crate) fn new(max_events: usize, max_event_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            max_events: max_events.max(1),
            max_event_bytes: max_event_bytes.max(1),
            delivered: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            lag_pending: AtomicUsize::new(0),
        }
    }

    fn record_drop(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
        self.lag_pending.fetch_add(1, Ordering::Relaxed);
    }

    /// Take and clear the pending-lag count. Returns `Some(n)` if `n > 0`
    /// evictions have accrued since the last take, else `None`. Coalesces a
    /// burst of drops into one `lagged` frame.
    pub(crate) fn take_pending_lag(&self) -> Option<usize> {
        match self.lag_pending.swap(0, Ordering::Relaxed) {
            0 => None,
            n => Some(n),
        }
    }

    /// Enqueue an event frame, evicting the oldest queued events as needed
    /// to respect the count and byte budgets. Evictions and an unqueueable
    /// oversized frame count as drops.
    pub(crate) fn push_event(&self, text: std::sync::Arc<str>) {
        if text.len() > self.max_event_bytes {
            self.record_drop();
            return;
        }
        {
            let mut frames = self.inner.lock().expect("outbound queue poisoned");
            while over_budget(&frames, text.len(), self.max_events, self.max_event_bytes) {
                let Some(evict_at) = frames.iter().position(|frame| frame.is_event) else {
                    break;
                };
                frames.remove(evict_at);
                self.record_drop();
            }
            frames.push_back(Frame {
                text,
                closing: false,
                is_event: true,
            });
        }
        self.notify.notify_waiters();
    }

    /// Enqueue a control frame; exempt from budgets and eviction.
    pub(crate) fn push_control(&self, msg: &whdr_proto::SubServerMsg) {
        let Ok(text) = serde_json::to_string(msg) else {
            return;
        };
        {
            let mut frames = self.inner.lock().expect("outbound queue poisoned");
            frames.push_back(Frame {
                text: text.into(),
                closing: matches!(msg, whdr_proto::SubServerMsg::Closing { .. }),
                is_event: false,
            });
        }
        self.notify.notify_waiters();
    }

    /// Await the next frame. Cancel-safe: intended for `tokio::select!`.
    pub(crate) async fn pop(&self) -> Frame {
        loop {
            let mut notified = pin!(self.notify.notified());
            notified.as_mut().enable();
            {
                let mut frames = self.inner.lock().expect("outbound queue poisoned");
                if let Some(frame) = frames.pop_front() {
                    if frame.is_event {
                        self.delivered.fetch_add(1, Ordering::Relaxed);
                    }
                    return frame;
                }
            }
            notified.await;
        }
    }

    /// Events handed to the connection writer so far.
    pub(crate) fn delivered(&self) -> usize {
        self.delivered.load(Ordering::Relaxed)
    }

    /// Events evicted or rejected due to the count/byte budgets.
    pub(crate) fn dropped(&self) -> usize {
        self.dropped.load(Ordering::Relaxed)
    }
}

fn over_budget(
    frames: &VecDeque<Frame>,
    incoming_bytes: usize,
    max_events: usize,
    max_event_bytes: usize,
) -> bool {
    let event_count = frames.iter().filter(|frame| frame.is_event).count();
    if event_count == 0 {
        return false;
    }
    let event_bytes: usize = frames
        .iter()
        .filter(|frame| frame.is_event)
        .map(|frame| frame.text.len())
        .sum();
    event_count + 1 > max_events || event_bytes + incoming_bytes > max_event_bytes
}

#[cfg(test)]
mod tests {
    use whdr_proto::SubServerMsg;

    use super::*;

    fn event(text: &str) -> std::sync::Arc<str> {
        text.into()
    }

    #[tokio::test]
    async fn pop_returns_frames_in_order() {
        let queue = OutboundQueue::new(8, 1024);
        queue.push_event(event("one"));
        queue.push_control(&SubServerMsg::Pong);

        assert_eq!(&*queue.pop().await.text, "one");
        assert_eq!(&*queue.pop().await.text, r#"{"type":"pong"}"#);
        assert_eq!(queue.delivered(), 1);
    }

    #[tokio::test]
    async fn count_overflow_evicts_oldest_event() {
        let queue = OutboundQueue::new(2, 1024);
        queue.push_event(event("a"));
        queue.push_event(event("b"));
        queue.push_event(event("c"));

        assert_eq!(&*queue.pop().await.text, "b");
        assert_eq!(&*queue.pop().await.text, "c");
        assert_eq!(queue.dropped(), 1);
    }

    #[tokio::test]
    async fn byte_overflow_evicts_oldest_until_incoming_fits() {
        let queue = OutboundQueue::new(64, 10);
        queue.push_event(event("aaaa"));
        queue.push_event(event("bbbb"));
        queue.push_event(event("cccccc"));

        assert_eq!(&*queue.pop().await.text, "bbbb");
        assert_eq!(&*queue.pop().await.text, "cccccc");
        assert_eq!(queue.dropped(), 1);
    }

    #[tokio::test]
    async fn oversized_event_is_rejected_not_queued() {
        let queue = OutboundQueue::new(64, 10);
        queue.push_event(event("this frame is far too large"));

        assert_eq!(queue.dropped(), 1);
        queue.push_event(event("fits"));
        assert_eq!(&*queue.pop().await.text, "fits");
    }

    #[tokio::test]
    async fn eviction_records_pending_lag_once_until_taken() {
        let queue = OutboundQueue::new(1, 1 << 20);
        queue.push_event(event("a"));
        queue.push_event(event("b")); // evicts "a" => 1 drop
        queue.push_event(event("c")); // evicts "b" => 2 drops
        assert_eq!(queue.take_pending_lag(), Some(2));
        assert_eq!(queue.take_pending_lag(), None, "coalesced; cleared after take");
    }

    #[tokio::test]
    async fn control_frames_are_never_evicted() {
        let queue = OutboundQueue::new(1, 4);
        queue.push_control(&SubServerMsg::Pong);
        queue.push_event(event("x"));
        queue.push_event(event("y"));

        assert_eq!(&*queue.pop().await.text, r#"{"type":"pong"}"#);
        assert_eq!(&*queue.pop().await.text, "y");
        assert_eq!(queue.dropped(), 1);
    }

    #[tokio::test]
    async fn closing_control_frame_is_flagged() {
        let queue = OutboundQueue::new(8, 1024);
        queue.push_control(&SubServerMsg::Closing {
            reason: whdr_proto::ClosingReason::Shutdown,
        });
        let frame = queue.pop().await;
        assert!(frame.closing);
    }

    #[tokio::test]
    async fn pop_wakes_when_a_frame_arrives() {
        let queue = std::sync::Arc::new(OutboundQueue::new(8, 1024));
        let waiter = {
            let queue = queue.clone();
            tokio::spawn(async move { queue.pop().await })
        };
        tokio::task::yield_now().await;
        queue.push_event(event("wake"));
        let frame = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("pop should wake")
            .unwrap();
        assert_eq!(&*frame.text, "wake");
    }
}
