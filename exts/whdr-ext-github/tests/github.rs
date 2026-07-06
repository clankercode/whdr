use std::collections::BTreeMap;

use whdr_ext_github::handle_github_dispatch;
use whdr_proto::{HttpReply, SrvMsg};

fn dispatch(body: &[u8], secret: &str, signature: &str, event: &str) -> SrvMsg {
    let mut headers = BTreeMap::new();
    headers.insert("x-hub-signature-256".to_string(), signature.to_string());
    headers.insert("x-github-event".to_string(), event.to_string());
    SrvMsg::Dispatch {
        req_id: uuid::Uuid::nil(),
        method: "POST".to_string(),
        path: "/github".to_string(),
        query: None,
        headers,
        body_b64: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, body),
        secret: Some(secret.to_string()),
    }
}

#[test]
fn valid_github_signature_emits_event_action_channel() {
    let body = br#"{"action":"opened","number":42}"#;
    let sig = whdr_ext_github::github_signature("whsec", body);
    let (reply, events) =
        handle_github_dispatch(dispatch(body, "whsec", &sig, "pull_request")).unwrap();

    assert_eq!(reply.status, 200);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].channel, "github.pull_request.opened");
    assert_eq!(
        events[0].payload_b64,
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, body)
    );
}

#[test]
fn invalid_github_signature_returns_401_without_events() {
    let (reply, events) = handle_github_dispatch(dispatch(
        br#"{"action":"opened"}"#,
        "whsec",
        "sha256:bad",
        "pull_request",
    ))
    .unwrap();
    assert_eq!(
        reply,
        HttpReply {
            status: 401,
            headers: BTreeMap::new(),
            body: "invalid signature".to_string()
        }
    );
    assert!(events.is_empty());
}

#[test]
fn wrong_signature_of_correct_length_is_rejected() {
    // A forged signature that matches the expected length exercises the
    // constant-time byte comparison (not just the length guard): flip the
    // last hex nibble of a valid signature and confirm it is still rejected.
    let body = br#"{"action":"opened"}"#;
    let valid = whdr_ext_github::github_signature("whsec", body);
    let mut forged = valid.clone();
    let last = forged.pop().unwrap();
    forged.push(if last == '0' { '1' } else { '0' });
    assert_eq!(forged.len(), valid.len());

    let (reply, events) =
        handle_github_dispatch(dispatch(body, "whsec", &forged, "pull_request")).unwrap();
    assert_eq!(reply.status, 401);
    assert!(events.is_empty());
}
