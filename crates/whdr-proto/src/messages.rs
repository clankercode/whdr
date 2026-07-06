use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub channel: String,
    pub payload_b64: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpReply {
    pub status: u16,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExtMsg {
    Register {
        protocol: u32,
        #[serde(default)]
        id: Option<String>,
        #[serde(default)]
        paths: Vec<String>,
        #[serde(default)]
        channels: Vec<String>,
        #[serde(default)]
        meta: Value,
    },
    Result {
        req_id: Uuid,
        http: HttpReply,
        #[serde(default)]
        events: Vec<Event>,
    },
    Event {
        #[serde(flatten)]
        ev: Event,
    },
    Log {
        level: LogLevel,
        msg: String,
    },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SrvMsg {
    Dispatch {
        req_id: Uuid,
        method: String,
        path: String,
        query: Option<String>,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        body_b64: String,
        secret: Option<String>,
    },
    Shutdown,
}

impl fmt::Debug for SrvMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SrvMsg::Dispatch {
                req_id,
                method,
                path,
                query,
                headers,
                body_b64,
                secret,
            } => f
                .debug_struct("Dispatch")
                .field("req_id", req_id)
                .field("method", method)
                .field("path", path)
                .field("query", query)
                .field("headers", headers)
                .field("body_b64", body_b64)
                .field("secret", &secret.as_ref().map(|_| "<redacted>"))
                .finish(),
            SrvMsg::Shutdown => f.write_str("Shutdown"),
        }
    }
}

/// Optional resume cursor on `subscribe`: the server streams stored events
/// with `seq > after_seq` matching the connection's patterns before live
/// delivery (§9.4). Client-owned; the server holds no per-subscriber offset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayRequest {
    pub after_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SubClientMsg {
    Subscribe {
        patterns: Vec<String>,
        #[serde(default)]
        replay: Option<ReplayRequest>,
    },
    Unsubscribe {
        patterns: Vec<String>,
    },
    Ping,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SubServerMsg {
    Welcome {
        name: String,
    },
    Ok {
        op: String,
    },
    Error {
        op: String,
        msg: String,
    },
    Event {
        id: Uuid,
        seq: u64,
        ts_ms: u64,
        channel: String,
        payload_b64: String,
    },
    /// Replay window fully delivered up to `through_seq`; live frames follow.
    Replayed {
        through_seq: u64,
    },
    /// The requested `after_seq` predates the retained floor; events in
    /// `(from_seq, earliest_seq)` are permanently pruned. Replay resumes
    /// from `earliest_seq`.
    ReplayGap {
        from_seq: u64,
        earliest_seq: u64,
    },
    /// The outbound queue evicted `dropped` events for this connection;
    /// reconnect and replay from your cursor to recover.
    Lagged {
        dropped: u64,
    },
    Pong,
    Closing {
        reason: ClosingReason,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClosingReason {
    Shutdown,
    Revoked,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlRequest {
    Status,
    #[serde(rename = "token.add")]
    TokenAdd {
        name: String,
    },
    #[serde(rename = "token.rotate")]
    TokenRotate {
        name: String,
    },
    #[serde(rename = "token.revoke")]
    TokenRevoke {
        name: String,
    },
    #[serde(rename = "token.list")]
    TokenList,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlResponse {
    Status { status: Value },
    Token { name: String, token: String },
    Tokens { tokens: Vec<TokenSummary> },
    Ok,
    Error { msg: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenSummary {
    pub name: String,
    pub fingerprint: String,
    pub created: String,
    pub active_conns: usize,
}
