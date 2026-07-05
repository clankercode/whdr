use std::collections::BTreeMap;

use whdr_ext_teams::handle_teams_dispatch;
use whdr_proto::SrvMsg;

#[test]
fn validation_token_echoes_text_plain() {
    let (reply, events) = handle_teams_dispatch(SrvMsg::Dispatch {
        req_id: uuid::Uuid::nil(),
        method: "GET".to_string(),
        path: "/teams".to_string(),
        query: Some("validationToken=abc123".to_string()),
        headers: BTreeMap::new(),
        body_b64: String::new(),
        secret: None,
    })
    .unwrap();

    assert_eq!(reply.status, 200);
    assert_eq!(reply.headers.get("content-type").unwrap(), "text/plain");
    assert_eq!(reply.body, "abc123");
    assert!(events.is_empty());
}

#[test]
fn graph_notification_resource_becomes_channel() {
    let body =
        br#"{"value":[{"clientState":"expected","resource":"teams/team-id/channels/general/messages"}]}"#;
    let (reply, events) = handle_teams_dispatch(SrvMsg::Dispatch {
        req_id: uuid::Uuid::nil(),
        method: "POST".to_string(),
        path: "/teams".to_string(),
        query: None,
        headers: BTreeMap::new(),
        body_b64: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, body),
        secret: Some("expected".to_string()),
    })
    .unwrap();

    assert_eq!(reply.status, 202);
    assert_eq!(
        events[0].channel,
        "teams.teams.team-id.channels.general.messages"
    );
}

#[test]
fn graph_notification_requires_matching_client_state_secret() {
    let body = br#"{"value":[{"clientState":"wrong","resource":"teams/team-id/channels/general/messages"}]}"#;
    let (reply, events) = handle_teams_dispatch(SrvMsg::Dispatch {
        req_id: uuid::Uuid::nil(),
        method: "POST".to_string(),
        path: "/teams".to_string(),
        query: None,
        headers: BTreeMap::new(),
        body_b64: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, body),
        secret: Some("expected".to_string()),
    })
    .unwrap();

    assert_eq!(reply.status, 401);
    assert!(events.is_empty());

    let body = br#"{"value":[{"clientState":"expected","resource":"teams/team-id/channels/general/messages"}]}"#;
    let (reply, events) = handle_teams_dispatch(SrvMsg::Dispatch {
        req_id: uuid::Uuid::nil(),
        method: "POST".to_string(),
        path: "/teams".to_string(),
        query: None,
        headers: BTreeMap::new(),
        body_b64: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, body),
        secret: Some("expected".to_string()),
    })
    .unwrap();

    assert_eq!(reply.status, 202);
    assert_eq!(events.len(), 1);
}
