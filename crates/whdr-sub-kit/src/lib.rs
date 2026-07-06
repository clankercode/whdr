//! `whdr-sub-kit` — the official Rust client library for the **whdr** subscriber
//! plane, and the reference implementation of the *Subscriber wire protocol v2*
//! (durable delivery / replay). Four other language libraries mirror its
//! behaviour.
//!
//! whdr fans provider-webhook events out to token-authenticated WebSocket
//! subscribers. With durable delivery enabled on the server, a subscriber can
//! **resume from a cursor** and replay events it missed while offline or after a
//! slow-consumer drop — at-least-once, deduplicated by event `id`.
//!
//! # Two ways to use it
//!
//! - **[`Client::run`]** — the batteries-included loop. Implement [`Handler`],
//!   hand it to `run`, and the kit performs the full reconnect-and-resume
//!   algorithm for you: auth → welcome → subscribe with `replay.after_seq =
//!   cursor` → dedup by `id` / `seq` → advance the cursor after each successful
//!   handle → recover from `lagged` / disconnects by reconnecting from the
//!   cursor → surface `replay_gap` → treat `revoked` as fatal and `shutdown` as
//!   a backoff reconnect. This is what most callers want.
//! - **[`Client::connect`]** — the typed event stream. Get a [`Connection`] and
//!   drive [`Connection::recv`] yourself, applying [`ResumeState`] for dedup.
//!   Use this when you need bespoke control over the loop.
//!
//! # Example
//!
//! ```no_run
//! use whdr_sub_kit::{Client, DeliveredEvent, Handler};
//!
//! struct Printer;
//!
//! #[async_trait::async_trait]
//! impl Handler for Printer {
//!     async fn on_event(&mut self, event: &DeliveredEvent) -> anyhow::Result<()> {
//!         let body = event.payload()?;
//!         println!("[{}] seq={} {} bytes", event.channel, event.seq, body.len());
//!         Ok(())
//!     }
//!
//!     async fn on_replay_gap(&mut self, from_seq: u64, earliest_seq: u64) -> anyhow::Result<()> {
//!         eprintln!("data loss: events ({from_seq}, {earliest_seq}) were pruned");
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let client = Client::builder("ws://127.0.0.1:8788/subscribe", "tok_your_token")
//!         .pattern("github.>")
//!         .resume_cursor(0) // 0 = replay from the start of retention
//!         .build();
//!
//!     // Runs forever, reconnecting with backoff. Returns only on a fatal
//!     // error (revoked token, auth failure, or a handler error).
//!     client.run(Printer).await?;
//!     Ok(())
//! }
//! ```
//!
//! # Conformance
//!
//! This kit implements the 10-point client-library conformance checklist from
//! the *Subscriber wire protocol v2* appendix. See `docs/SUBSCRIBERS.md` for a
//! quickstart (minting a token and running a subscriber).

mod backoff;
mod connection;
mod cursor;
mod error;
mod frame;
mod resume;

use std::sync::Arc;

use async_trait::async_trait;

pub use backoff::{Backoff, BackoffPolicy};
pub use connection::{Connection, build_request, subscribe_msg};
pub use cursor::{CursorStore, MemoryCursorStore};
pub use error::Error;
pub use frame::{DeliveredEvent, WsAction, classify, closing_is_fatal, parse_frame};
pub use resume::ResumeState;

// Re-export the wire types so callers don't need a direct `whdr-proto` dep.
pub use whdr_proto::{ClosingReason, ReplayRequest, SubClientMsg, SubServerMsg};

use frame::closing_is_fatal as reason_is_fatal;

/// Handler for the [`Client::run`] loop. Only [`Handler::on_event`] is
/// required; the signal hooks default to logging (or nothing).
///
/// Returning `Err` from any hook is **fatal**: `run` stops and returns
/// [`Error::Handler`]. The cursor is advanced (and persisted) only *after*
/// [`on_event`](Handler::on_event) returns `Ok`, giving at-least-once delivery.
#[async_trait]
pub trait Handler: Send {
    /// Handle a delivered event. The kit has already de-duplicated by `id` and
    /// `seq`, so this is called at most once per event. On `Ok`, the cursor
    /// advances to `event.seq`.
    async fn on_event(&mut self, event: &DeliveredEvent) -> anyhow::Result<()>;

    /// A replay window finished; live frames follow. `through_seq` is the head
    /// the connection caught up to. Default: no-op.
    async fn on_replayed(&mut self, _through_seq: u64) -> anyhow::Result<()> {
        Ok(())
    }

    /// **Explicit data-loss signal.** The requested cursor predated retention;
    /// events in `(from_seq, earliest_seq)` are permanently gone. Default:
    /// logs a warning. Override to alert or reconcile out-of-band.
    async fn on_replay_gap(&mut self, from_seq: u64, earliest_seq: u64) -> anyhow::Result<()> {
        tracing::warn!(
            from_seq,
            earliest_seq,
            "replay_gap: events were pruned before this subscriber resumed"
        );
        Ok(())
    }

    /// The server evicted `dropped` events for this connection. The kit will
    /// reconnect and replay from the cursor to recover. Default: no-op.
    async fn on_lagged(&mut self, _dropped: u64) -> anyhow::Result<()> {
        Ok(())
    }

    /// A `replay` request was refused because durable delivery is disabled on
    /// the server; live delivery still works. Default: no-op.
    async fn on_replay_unavailable(&mut self, _msg: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

/// A configured subscriber client. Build one with [`Client::builder`].
pub struct Client {
    url: String,
    token: String,
    patterns: Vec<String>,
    backoff: BackoffPolicy,
    cursor_store: Arc<dyn CursorStore>,
    dedup_capacity: usize,
}

impl Client {
    /// Start building a client for the `/subscribe` endpoint `url`, authenticating
    /// with `token`.
    pub fn builder(url: impl Into<String>, token: impl Into<String>) -> ClientBuilder {
        ClientBuilder {
            url: url.into(),
            token: token.into(),
            patterns: Vec::new(),
            backoff: BackoffPolicy::default(),
            cursor_store: None,
            resume_cursor: 0,
            dedup_capacity: 8192,
        }
    }

    /// Connect, authenticate, and subscribe with the configured patterns and
    /// cursor (loaded from the cursor store). Returns a ready [`Connection`]
    /// whose [`recv`](Connection::recv) yields the typed event stream.
    ///
    /// Use this for bespoke loops; most callers want [`Client::run`].
    pub async fn connect(&self) -> Result<Connection, Error> {
        let cursor = self.cursor_store.load().await?;
        let mut conn = Connection::connect(&self.url, &self.token).await?;
        conn.subscribe(&self.patterns, Some(cursor)).await?;
        Ok(conn)
    }

    /// Run the full reconnect-and-resume loop, driving `handler`.
    ///
    /// Loops forever, reconnecting with exponential backoff after a transient
    /// failure (dropped socket, server `shutdown`, `lagged` eviction). Returns
    /// only on a **fatal** error: a revoked/absent token ([`Error::Auth`] /
    /// [`Error::Revoked`]) or a handler failure ([`Error::Handler`]).
    pub async fn run(&self, mut handler: impl Handler) -> Result<(), Error> {
        let cursor = self.cursor_store.load().await?;
        let mut resume = ResumeState::new(cursor, self.dedup_capacity);
        let mut backoff = self.backoff.start();
        loop {
            match self
                .run_session(&mut handler, &mut resume, &mut backoff)
                .await
            {
                Ok(()) => tracing::info!("subscriber session ended; reconnecting"),
                Err(err) if err.is_fatal() => return Err(err),
                Err(err) => tracing::warn!(error = %err, "subscriber session error; reconnecting"),
            }
            let delay = backoff.next_delay();
            tracing::debug!(?delay, "backing off before reconnect");
            tokio::time::sleep(delay).await;
        }
    }

    /// One connection's lifetime. `Ok(())` means "reconnect and resume" (clean
    /// close, `shutdown`, or `lagged`); a fatal `Err` stops the loop.
    async fn run_session(
        &self,
        handler: &mut impl Handler,
        resume: &mut ResumeState,
        backoff: &mut Backoff,
    ) -> Result<(), Error> {
        let mut conn = Connection::connect(&self.url, &self.token).await?;
        // Connected successfully: reset backoff so a later drop reconnects fast.
        backoff.reset();
        conn.subscribe(&self.patterns, Some(resume.cursor()))
            .await?;

        loop {
            match conn.recv().await? {
                SubServerMsg::Event {
                    id,
                    seq,
                    ts_ms,
                    channel,
                    payload_b64,
                } => {
                    if resume.should_process(id, seq) {
                        let event = DeliveredEvent {
                            id,
                            seq,
                            ts_ms,
                            channel,
                            payload_b64,
                        };
                        handler.on_event(&event).await.map_err(Error::Handler)?;
                        resume.record(event.id, event.seq);
                        self.cursor_store.save(resume.cursor()).await?;
                    }
                }
                SubServerMsg::Replayed { through_seq } => {
                    handler
                        .on_replayed(through_seq)
                        .await
                        .map_err(Error::Handler)?;
                }
                SubServerMsg::ReplayGap {
                    from_seq,
                    earliest_seq,
                } => {
                    handler
                        .on_replay_gap(from_seq, earliest_seq)
                        .await
                        .map_err(Error::Handler)?;
                }
                SubServerMsg::Lagged { dropped } => {
                    handler.on_lagged(dropped).await.map_err(Error::Handler)?;
                    // Recover by reconnecting and replaying from the cursor.
                    return Ok(());
                }
                SubServerMsg::Error { op, msg } => {
                    if op == "replay" {
                        tracing::warn!(%msg, "replay refused (durability disabled)");
                        handler
                            .on_replay_unavailable(&msg)
                            .await
                            .map_err(Error::Handler)?;
                    } else {
                        tracing::warn!(%op, %msg, "server error frame");
                    }
                }
                SubServerMsg::Closing { reason } => {
                    if reason_is_fatal(&reason) {
                        return Err(Error::Revoked);
                    }
                    // shutdown: reconnect with backoff.
                    return Ok(());
                }
                // welcome (unexpected repeat), ok, pong: nothing to do.
                SubServerMsg::Welcome { .. } | SubServerMsg::Ok { .. } | SubServerMsg::Pong => {}
            }
        }
    }
}

/// Builder for a [`Client`].
pub struct ClientBuilder {
    url: String,
    token: String,
    patterns: Vec<String>,
    backoff: BackoffPolicy,
    cursor_store: Option<Arc<dyn CursorStore>>,
    resume_cursor: u64,
    dedup_capacity: usize,
}

impl ClientBuilder {
    /// Add one channel pattern (NATS-style, e.g. `github.>`).
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.patterns.push(pattern.into());
        self
    }

    /// Add several channel patterns.
    pub fn patterns<I, S>(mut self, patterns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.patterns.extend(patterns.into_iter().map(Into::into));
        self
    }

    /// Override the reconnect backoff policy.
    pub fn backoff(mut self, policy: BackoffPolicy) -> Self {
        self.backoff = policy;
        self
    }

    /// Seed the resume cursor (ignored if a [`cursor_store`](Self::cursor_store)
    /// is set, which supplies the cursor via [`CursorStore::load`]). `0` replays
    /// from the start of retention.
    pub fn resume_cursor(mut self, cursor: u64) -> Self {
        self.resume_cursor = cursor;
        self
    }

    /// Install a cursor-persistence hook (for at-least-once across restarts).
    pub fn cursor_store(mut self, store: Arc<dyn CursorStore>) -> Self {
        self.cursor_store = Some(store);
        self
    }

    /// Set the recent-`id` dedup window size (default 8192).
    pub fn dedup_capacity(mut self, capacity: usize) -> Self {
        self.dedup_capacity = capacity.max(1);
        self
    }

    /// Finish building the [`Client`].
    pub fn build(self) -> Client {
        let cursor_store = self
            .cursor_store
            .unwrap_or_else(|| Arc::new(MemoryCursorStore::new(self.resume_cursor)));
        Client {
            url: self.url,
            token: self.token,
            patterns: self.patterns,
            backoff: self.backoff,
            cursor_store,
            dedup_capacity: self.dedup_capacity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn builder_defaults_and_overrides() {
        let client = Client::builder("ws://x/subscribe", "tok_x")
            .pattern("a.>")
            .patterns(["b.>", "c.>"])
            .resume_cursor(9)
            .dedup_capacity(4)
            .build();
        assert_eq!(client.patterns, vec!["a.>", "b.>", "c.>"]);
        assert_eq!(client.dedup_capacity, 4);
        // resume_cursor seeds the default memory store.
        assert_eq!(client.cursor_store.load().await.unwrap(), 9);
    }

    #[tokio::test]
    async fn explicit_cursor_store_overrides_resume_seed() {
        let store = Arc::new(MemoryCursorStore::new(500));
        let client = Client::builder("ws://x/subscribe", "tok_x")
            .resume_cursor(9)
            .cursor_store(store.clone())
            .build();
        assert_eq!(client.cursor_store.load().await.unwrap(), 500);
    }
}
