//! The typed WebSocket connection: authenticated upgrade, welcome handshake,
//! and a frame-by-frame typed stream.

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use whdr_proto::{ReplayRequest, SubClientMsg, SubServerMsg};

use crate::error::Error;
use crate::frame::{WsAction, classify, parse_frame};

type Stream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Build the authenticated WebSocket upgrade request: `url` plus an
/// `Authorization: Bearer <token>` header (conformance item 1).
pub fn build_request(url: &str, token: &str) -> Result<Request, Error> {
    let mut request = url
        .into_client_request()
        .map_err(|err| Error::Request(err.to_string()))?;
    let value = format!("Bearer {token}")
        .parse()
        .map_err(|_| Error::Request("invalid Authorization header value".to_string()))?;
    request.headers_mut().insert("Authorization", value);
    Ok(request)
}

/// An authenticated subscriber connection, positioned just after the `welcome`
/// frame. Yields typed [`SubServerMsg`] frames via [`Connection::recv`],
/// transparently answering WebSocket pings and skipping unknown frames.
pub struct Connection {
    stream: Stream,
    name: String,
}

impl Connection {
    /// Connect, authenticate, and consume the `welcome` frame.
    ///
    /// Waits for `welcome` before returning (conformance item 2). A `401`
    /// upgrade rejection maps to [`Error::Auth`] (conformance item 1).
    pub async fn connect(url: &str, token: &str) -> Result<Self, Error> {
        let request = build_request(url, token)?;
        let (stream, _response) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(map_connect_error)?;
        let mut conn = Self {
            stream,
            name: String::new(),
        };
        // Read frames until the welcome; anything else before it is skipped.
        loop {
            match conn.recv().await? {
                SubServerMsg::Welcome { name } => {
                    conn.name = name;
                    return Ok(conn);
                }
                other => {
                    tracing::debug!(?other, "frame before welcome; ignoring");
                }
            }
        }
    }

    /// The subscriber name echoed in the `welcome` frame (the token's label).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Send a `subscribe`, optionally resuming from `after_seq` (conformance
    /// item 3: always resume with `replay.after_seq = cursor`).
    pub async fn subscribe(
        &mut self,
        patterns: &[String],
        after_seq: Option<u64>,
    ) -> Result<(), Error> {
        let msg = subscribe_msg(patterns, after_seq);
        self.send(&msg).await
    }

    /// Send an application-level `ping` (`{"type":"ping"}`).
    pub async fn ping(&mut self) -> Result<(), Error> {
        self.send(&SubClientMsg::Ping).await
    }

    /// Send a client message.
    pub async fn send(&mut self, msg: &SubClientMsg) -> Result<(), Error> {
        let text = serde_json::to_string(msg).expect("SubClientMsg serialises");
        self.stream
            .send(Message::Text(text.into()))
            .await
            .map_err(|err| Error::Transport(Box::new(err)))
    }

    /// Read the next typed server frame.
    ///
    /// Answers WebSocket pings inline (conformance item 9) and skips
    /// unrecognised frames (conformance item 10). Returns
    /// [`Error::ConnectionClosed`] when the peer closes.
    pub async fn recv(&mut self) -> Result<SubServerMsg, Error> {
        loop {
            let message = match self.stream.next().await {
                Some(Ok(message)) => message,
                Some(Err(err)) => return Err(Error::Transport(Box::new(err))),
                None => return Err(Error::ConnectionClosed),
            };
            match classify(&message) {
                WsAction::Text(text) => {
                    if let Some(msg) = parse_frame(&text) {
                        return Ok(msg);
                    }
                    // Unknown frame: keep reading.
                }
                WsAction::Pong(payload) => {
                    self.stream
                        .send(Message::Pong(payload.into()))
                        .await
                        .map_err(|err| Error::Transport(Box::new(err)))?;
                }
                WsAction::Closed => return Err(Error::ConnectionClosed),
                WsAction::Ignore => {}
            }
        }
    }
}

/// Build a `subscribe` client message, attaching a resume cursor when
/// `after_seq` is `Some`.
pub fn subscribe_msg(patterns: &[String], after_seq: Option<u64>) -> SubClientMsg {
    SubClientMsg::Subscribe {
        patterns: patterns.to_vec(),
        replay: after_seq.map(|after_seq| ReplayRequest { after_seq }),
    }
}

/// Map a `connect_async` error, translating a `401` upgrade rejection to
/// [`Error::Auth`] and other HTTP statuses to [`Error::Http`].
fn map_connect_error(err: WsError) -> Error {
    match err {
        WsError::Http(response) => {
            let status = response.status().as_u16();
            if status == 401 {
                Error::Auth
            } else {
                Error::Http(status)
            }
        }
        other => Error::Transport(Box::new(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_request_sets_bearer_header() {
        // Conformance item 1: Authorization: Bearer on the upgrade.
        let request = build_request("ws://127.0.0.1:8788/subscribe", "tok_abc").unwrap();
        let auth = request.headers().get("Authorization").unwrap();
        assert_eq!(auth, "Bearer tok_abc");
    }

    #[test]
    fn subscribe_msg_uses_cursor_as_after_seq() {
        // Conformance item 3: resume with replay.after_seq = cursor.
        let patterns = vec!["github.>".to_string()];
        match subscribe_msg(&patterns, Some(128)) {
            SubClientMsg::Subscribe { patterns, replay } => {
                assert_eq!(patterns, vec!["github.>".to_string()]);
                assert_eq!(replay.unwrap().after_seq, 128);
            }
            other => panic!("expected subscribe, got {other:?}"),
        }
        // Live-only when no cursor.
        match subscribe_msg(&patterns, None) {
            SubClientMsg::Subscribe { replay, .. } => assert!(replay.is_none()),
            other => panic!("expected subscribe, got {other:?}"),
        }
    }

    #[test]
    fn maps_401_to_auth_error() {
        // Conformance item 1: 401 is fatal.
        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(401)
            .body(None)
            .unwrap();
        assert!(matches!(
            map_connect_error(WsError::Http(response)),
            Error::Auth
        ));
        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(503)
            .body(None)
            .unwrap();
        assert!(matches!(
            map_connect_error(WsError::Http(response)),
            Error::Http(503)
        ));
    }
}
