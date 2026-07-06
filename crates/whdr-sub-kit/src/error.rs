//! Error type for the subscriber client.

use thiserror::Error;

/// Errors surfaced by the subscriber client.
///
/// [`Error::is_fatal`] distinguishes *fatal* errors (the [`run`](crate::Client::run)
/// loop stops and returns them) from *transient* errors (the loop reconnects with
/// backoff). The reconnect-and-resume algorithm (SPEC §9.4) treats an authentication
/// failure, a `revoked` close, and a handler error as fatal; everything else — a
/// dropped socket, a server `shutdown`, a `lagged` eviction — is transient.
#[derive(Debug, Error)]
pub enum Error {
    /// The WebSocket upgrade was rejected with HTTP `401`. The token is
    /// wrong or revoked. **Fatal.**
    #[error("authentication failed (HTTP 401): token missing, wrong, or revoked")]
    Auth,

    /// The WebSocket upgrade failed with a non-401 HTTP status.
    #[error("websocket upgrade failed with HTTP {0}")]
    Http(u16),

    /// The server sent `closing` with reason `revoked`: the token was
    /// rotated or revoked mid-connection. Obtain a new token. **Fatal.**
    #[error("connection closed by server: token revoked")]
    Revoked,

    /// The application event handler returned an error. **Fatal.**
    #[error("event handler failed: {0}")]
    Handler(#[source] anyhow::Error),

    /// A cursor-persistence hook failed. **Fatal** (a client that cannot
    /// persist its cursor cannot honour its at-least-once contract).
    #[error("cursor store failed: {0}")]
    CursorStore(#[source] anyhow::Error),

    /// The connection closed or the transport errored. **Transient.**
    #[error("websocket transport error: {0}")]
    Transport(#[source] Box<tokio_tungstenite::tungstenite::Error>),

    /// The connection closed cleanly (or with a `Close` frame carrying no
    /// actionable reason). **Transient.**
    #[error("connection closed")]
    ConnectionClosed,

    /// Building the connection request (URL / header) failed. **Fatal.**
    #[error("invalid connection request: {0}")]
    Request(String),
}

impl Error {
    /// Whether the [`run`](crate::Client::run) loop should stop and return this
    /// error instead of reconnecting.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Error::Auth
                | Error::Revoked
                | Error::Handler(_)
                | Error::CursorStore(_)
                | Error::Request(_)
        )
    }
}
