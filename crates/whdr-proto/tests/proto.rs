use std::collections::BTreeMap;

use uuid::Uuid;
use whdr_proto::{
    ExtMsg, HttpReply, SrvMsg, SubServerMsg, channel_matches, decode_line, encode_line,
    validate_channel, validate_pattern,
};

#[test]
fn ext_and_server_messages_round_trip_as_snake_case_ndjson() {
    let req_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa").unwrap();
    let mut headers = BTreeMap::new();
    headers.insert("content-type".to_string(), "text/plain".to_string());

    let ext = ExtMsg::Result {
        req_id,
        http: HttpReply {
            status: 202,
            headers,
            body: "accepted".to_string(),
        },
        events: vec![],
    };
    let line = encode_line(&ext).unwrap();

    assert!(line.ends_with('\n'));
    assert!(line.contains(r#""type":"result""#));
    assert_eq!(decode_line::<ExtMsg>(&line).unwrap(), Some(ext));

    let srv = SrvMsg::Dispatch {
        req_id,
        method: "POST".to_string(),
        path: "/github/hooks".to_string(),
        query: Some("x=1".to_string()),
        headers: BTreeMap::new(),
        body_b64: "e30=".to_string(),
        secret: Some("super-secret".to_string()),
    };
    assert_eq!(
        decode_line::<SrvMsg>(&encode_line(&srv).unwrap()).unwrap(),
        Some(srv)
    );
}

#[test]
fn subscriber_event_frame_carries_id_and_timestamp() {
    let msg = SubServerMsg::Event {
        id: Uuid::parse_str("bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb").unwrap(),
        ts_ms: 1_751_760_000_000,
        channel: "github.push".to_string(),
        payload_b64: "e30=".to_string(),
    };

    let line = encode_line(&msg).unwrap();
    assert!(line.contains(r#""type":"event""#));
    assert!(line.contains(r#""id":"bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb""#));
    assert!(line.contains(r#""ts_ms":1751760000000"#));
    assert_eq!(decode_line::<SubServerMsg>(&line).unwrap(), Some(msg));
}

#[test]
fn ndjson_skips_blank_lines_and_reports_malformed_json() {
    assert_eq!(decode_line::<ExtMsg>("   \n").unwrap(), None);
    let err = decode_line::<ExtMsg>("{not-json}\n").unwrap_err();
    assert!(err.to_string().contains("malformed json"));
}

#[test]
fn dispatch_debug_redacts_secret() {
    let msg = SrvMsg::Dispatch {
        req_id: Uuid::nil(),
        method: "POST".to_string(),
        path: "/github".to_string(),
        query: None,
        headers: BTreeMap::new(),
        body_b64: "e30=".to_string(),
        secret: Some("top-secret-token".to_string()),
    };

    let rendered = format!("{msg:?}");
    assert!(rendered.contains("<redacted>"));
    assert!(!rendered.contains("top-secret-token"));
}

#[test]
fn channel_and_pattern_grammar_matches_spec() {
    assert!(validate_channel("github.push").is_ok());
    assert!(validate_channel("github.pr_opened-1").is_ok());
    assert!(validate_channel("github.*").is_err());

    assert!(validate_pattern(">").is_ok());
    assert!(validate_pattern("github.>").is_ok());
    assert!(validate_pattern("github.*").is_ok());
    assert!(validate_pattern("github.>x").is_err());

    assert!(channel_matches("github.push", "github.push").unwrap());
    assert!(channel_matches("github.push", "github.*").unwrap());
    assert!(!channel_matches("github.pr.opened", "github.*").unwrap());
    assert!(channel_matches("github.pr.opened", "github.>").unwrap());
    assert!(channel_matches("github.pr.opened", ">").unwrap());
    assert!(!channel_matches("stripe.charge", "github.>").unwrap());
}
