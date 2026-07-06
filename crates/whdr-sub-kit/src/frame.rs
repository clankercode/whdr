//! Frame parsing, WebSocket message classification, and the delivered-event
//! view. These are the pure, transport-agnostic building blocks the
//! [`Connection`](crate::Connection) and [`run`](crate::Client::run) loop are
//! built from — kept free of I/O so every conformance rule has a unit test.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;
use whdr_proto::{ClosingReason, SubServerMsg};

/// A delivered event, decoded from a [`SubServerMsg::Event`] frame.
///
/// The `id` is stable across live delivery and every replay of the event —
/// **dedup by `id`**. `seq` is the global monotonic cursor key. `ts_ms` is the
/// server wall-clock at fan-out; it is informational — order by `seq`, not
/// `ts_ms`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveredEvent {
    /// Stable event identity (dedup key).
    pub id: Uuid,
    /// Global monotonic sequence (cursor key).
    pub seq: u64,
    /// Server wall-clock at fan-out (unix ms; informational).
    pub ts_ms: u64,
    /// Channel the event was published on.
    pub channel: String,
    /// Standard-base64 of the raw event bytes.
    pub payload_b64: String,
}

impl DeliveredEvent {
    /// Decode `payload_b64` to raw bytes.
    pub fn payload(&self) -> Result<Vec<u8>, base64::DecodeError> {
        STANDARD.decode(&self.payload_b64)
    }
}

/// Parse one text frame into a typed message, **skipping** frames the kit does
/// not recognise.
///
/// Returns `None` for unknown `type` tags (forward compatibility, conformance
/// item 10) and for otherwise-undecodable frames; the caller ignores the frame
/// and reads the next one. Unknown *fields* on a known frame are tolerated
/// natively by `whdr-proto` (its enums are not `deny_unknown_fields`).
pub fn parse_frame(text: &str) -> Option<SubServerMsg> {
    match serde_json::from_str::<SubServerMsg>(text) {
        Ok(msg) => Some(msg),
        Err(err) => {
            tracing::debug!(%err, frame = %text, "ignoring unrecognised subscriber frame");
            None
        }
    }
}

/// What to do with a raw WebSocket message before it reaches frame parsing.
#[derive(Debug, PartialEq, Eq)]
pub enum WsAction {
    /// A text frame carrying JSON to parse.
    Text(String),
    /// A WebSocket ping; reply with this pong payload (conformance item 9).
    Pong(Vec<u8>),
    /// The peer closed the connection.
    Closed,
    /// Anything else (pong, binary, raw frame): ignore and keep reading.
    Ignore,
}

/// Classify a raw WebSocket message. Pure so the ping→pong rule is unit-tested
/// without a live socket.
pub fn classify(message: &Message) -> WsAction {
    match message {
        Message::Text(text) => WsAction::Text(text.to_string()),
        Message::Ping(payload) => WsAction::Pong(payload.to_vec()),
        Message::Close(_) => WsAction::Closed,
        _ => WsAction::Ignore,
    }
}

/// Whether a `closing` frame is fatal to the [`run`](crate::Client::run) loop.
///
/// `revoked` is fatal (obtain a new token); `shutdown` is a transient signal to
/// reconnect with backoff (conformance item 8).
pub fn closing_is_fatal(reason: &ClosingReason) -> bool {
    matches!(reason, ClosingReason::Revoked)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_event_frame() {
        let text = r#"{"type":"event","id":"00000000-0000-0000-0000-000000000000",
            "seq":7,"ts_ms":1751760000000,"channel":"github.push","payload_b64":"AA=="}"#;
        match parse_frame(text) {
            Some(SubServerMsg::Event { seq, channel, .. }) => {
                assert_eq!(seq, 7);
                assert_eq!(channel, "github.push");
            }
            other => panic!("expected event, got {other:?}"),
        }
    }

    #[test]
    fn skips_unknown_frame_type() {
        // Conformance item 10: unknown `type` values are ignored.
        assert!(parse_frame(r#"{"type":"quantum_flux","foo":1}"#).is_none());
        assert!(parse_frame("not json at all").is_none());
    }

    #[test]
    fn tolerates_unknown_fields_on_known_frame() {
        // Conformance item 10: unknown object fields are ignored.
        let text = r#"{"type":"welcome","name":"p","future_field":{"nested":true}}"#;
        match parse_frame(text) {
            Some(SubServerMsg::Welcome { name }) => assert_eq!(name, "p"),
            other => panic!("expected welcome, got {other:?}"),
        }
    }

    #[test]
    fn ping_classifies_as_pong() {
        // Conformance item 9: answer WebSocket ping frames.
        let ping = Message::Ping(vec![1, 2, 3].into());
        assert_eq!(classify(&ping), WsAction::Pong(vec![1, 2, 3]));
    }

    #[test]
    fn text_and_close_classify() {
        assert_eq!(
            classify(&Message::Text("hi".into())),
            WsAction::Text("hi".to_string())
        );
        assert_eq!(classify(&Message::Close(None)), WsAction::Closed);
        assert_eq!(classify(&Message::Pong(vec![].into())), WsAction::Ignore);
    }

    #[test]
    fn closing_reason_fatality() {
        // Conformance item 8.
        assert!(closing_is_fatal(&ClosingReason::Revoked));
        assert!(!closing_is_fatal(&ClosingReason::Shutdown));
    }

    #[test]
    fn delivered_event_decodes_payload() {
        let ev = DeliveredEvent {
            id: Uuid::nil(),
            seq: 1,
            ts_ms: 0,
            channel: "dev.x".into(),
            payload_b64: STANDARD.encode(b"hello"),
        };
        assert_eq!(ev.payload().unwrap(), b"hello");
    }
}
