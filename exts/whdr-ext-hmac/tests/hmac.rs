use std::collections::BTreeMap;

use whdr_ext_hmac::{Algorithm, Encoding, HmacConfig, handle_hmac_dispatch, sign};
use whdr_proto::SrvMsg;

const SECRET: &str = "whsec_topsecret";
const BODY: &[u8] = br#"{"event":"payment","id":"evt_123"}"#;

fn config(algorithm: Algorithm, encoding: Encoding, prefix: Option<&str>) -> HmacConfig {
    HmacConfig {
        header: "X-Signature".to_string(),
        algorithm,
        encoding,
        prefix: prefix.map(str::to_string),
        channel_prefix: "hmac".to_string(),
    }
}

fn dispatch(header: &str, value: &str, path: &str, secret: Option<&str>) -> SrvMsg {
    let mut headers = BTreeMap::new();
    headers.insert(header.to_string(), value.to_string());
    SrvMsg::Dispatch {
        req_id: uuid::Uuid::nil(),
        method: "POST".to_string(),
        path: path.to_string(),
        query: None,
        headers,
        body_b64: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, BODY),
        secret: secret.map(str::to_string),
    }
}

fn body_b64() -> String {
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, BODY)
}

// ------------------------------------------------------------ valid: algos

#[test]
fn sha256_hex_default_accepts_and_emits_event() {
    let cfg = config(Algorithm::Sha256, Encoding::Hex, None);
    let sig = sign(&cfg, SECRET, BODY);
    let (reply, events) =
        handle_hmac_dispatch(&cfg, dispatch("X-Signature", &sig, "/hmac", Some(SECRET))).unwrap();

    assert_eq!(reply.status, 200);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].channel, "hmac");
    assert_eq!(events[0].payload_b64, body_b64());
}

#[test]
fn sha1_hex_accepts() {
    let cfg = config(Algorithm::Sha1, Encoding::Hex, None);
    let sig = sign(&cfg, SECRET, BODY);
    let (reply, events) =
        handle_hmac_dispatch(&cfg, dispatch("X-Signature", &sig, "/hmac", Some(SECRET))).unwrap();
    assert_eq!(reply.status, 200);
    assert_eq!(events.len(), 1);
}

#[test]
fn sha512_hex_accepts() {
    let cfg = config(Algorithm::Sha512, Encoding::Hex, None);
    let sig = sign(&cfg, SECRET, BODY);
    let (reply, events) =
        handle_hmac_dispatch(&cfg, dispatch("X-Signature", &sig, "/hmac", Some(SECRET))).unwrap();
    assert_eq!(reply.status, 200);
    assert_eq!(events.len(), 1);
}

// ------------------------------------------------------------ valid: encoding

#[test]
fn base64_encoding_accepts() {
    let cfg = config(Algorithm::Sha256, Encoding::Base64, None);
    let sig = sign(&cfg, SECRET, BODY);
    // base64 signatures contain non-hex chars; ensure round-trips
    let (reply, events) =
        handle_hmac_dispatch(&cfg, dispatch("X-Signature", &sig, "/hmac", Some(SECRET))).unwrap();
    assert_eq!(reply.status, 200);
    assert_eq!(events.len(), 1);
}

// ------------------------------------------------------------ valid: prefix

#[test]
fn prefix_is_stripped_before_verify() {
    let cfg = config(Algorithm::Sha256, Encoding::Hex, Some("sha256="));
    let sig = sign(&cfg, SECRET, BODY);
    assert!(sig.starts_with("sha256="), "sign must include the prefix");
    let (reply, events) =
        handle_hmac_dispatch(&cfg, dispatch("X-Signature", &sig, "/hmac", Some(SECRET))).unwrap();
    assert_eq!(reply.status, 200);
    assert_eq!(events.len(), 1);
}

#[test]
fn custom_header_name_is_honoured() {
    let mut cfg = config(Algorithm::Sha256, Encoding::Hex, None);
    cfg.header = "X-Hub-Signature".to_string();
    let sig = sign(&cfg, SECRET, BODY);
    // header lookup is case-insensitive
    let (reply, _) = handle_hmac_dispatch(
        &cfg,
        dispatch("x-hub-signature", &sig, "/hmac", Some(SECRET)),
    )
    .unwrap();
    assert_eq!(reply.status, 200);
}

// ------------------------------------------------------------ channel derivation

#[test]
fn path_suffix_becomes_channel_segments() {
    let cfg = config(Algorithm::Sha256, Encoding::Hex, None);
    let sig = sign(&cfg, SECRET, BODY);
    let (_, events) = handle_hmac_dispatch(
        &cfg,
        dispatch("X-Signature", &sig, "/hmac/Stripe/Foo", Some(SECRET)),
    )
    .unwrap();
    assert_eq!(events[0].channel, "hmac.stripe.foo");
}

#[test]
fn channel_prefix_config_is_used() {
    let mut cfg = config(Algorithm::Sha256, Encoding::Hex, None);
    cfg.channel_prefix = "stripe".to_string();
    let sig = sign(&cfg, SECRET, BODY);
    let (_, events) = handle_hmac_dispatch(
        &cfg,
        dispatch("X-Signature", &sig, "/stripe/live", Some(SECRET)),
    )
    .unwrap();
    assert_eq!(events[0].channel, "stripe.live");
}

// ------------------------------------------------------------ rejections

#[test]
fn missing_header_is_rejected() {
    let cfg = config(Algorithm::Sha256, Encoding::Hex, None);
    let mut msg_headers = BTreeMap::new();
    msg_headers.insert("unrelated".to_string(), "x".to_string());
    let msg = SrvMsg::Dispatch {
        req_id: uuid::Uuid::nil(),
        method: "POST".to_string(),
        path: "/hmac".to_string(),
        query: None,
        headers: msg_headers,
        body_b64: body_b64(),
        secret: Some(SECRET.to_string()),
    };
    let (reply, events) = handle_hmac_dispatch(&cfg, msg).unwrap();
    assert_eq!(reply.status, 401);
    assert!(events.is_empty());
}

#[test]
fn missing_secret_is_rejected() {
    let cfg = config(Algorithm::Sha256, Encoding::Hex, None);
    let sig = sign(&cfg, SECRET, BODY);
    let (reply, events) =
        handle_hmac_dispatch(&cfg, dispatch("X-Signature", &sig, "/hmac", None)).unwrap();
    assert_eq!(reply.status, 401);
    assert!(events.is_empty());
}

#[test]
fn malformed_hex_signature_is_rejected() {
    let cfg = config(Algorithm::Sha256, Encoding::Hex, None);
    let (reply, events) = handle_hmac_dispatch(
        &cfg,
        dispatch("X-Signature", "zzzznothex", "/hmac", Some(SECRET)),
    )
    .unwrap();
    assert_eq!(reply.status, 401);
    assert!(events.is_empty());
}

#[test]
fn missing_expected_prefix_is_rejected() {
    let cfg = config(Algorithm::Sha256, Encoding::Hex, Some("sha256="));
    // valid hex but without the required prefix
    let raw = sign(
        &config(Algorithm::Sha256, Encoding::Hex, None),
        SECRET,
        BODY,
    );
    let (reply, events) =
        handle_hmac_dispatch(&cfg, dispatch("X-Signature", &raw, "/hmac", Some(SECRET))).unwrap();
    assert_eq!(reply.status, 401);
    assert!(events.is_empty());
}

#[test]
fn wrong_length_signature_is_rejected() {
    let cfg = config(Algorithm::Sha256, Encoding::Hex, None);
    // valid hex, but too short (sha256 hmac is 32 bytes / 64 hex chars)
    let (reply, events) = handle_hmac_dispatch(
        &cfg,
        dispatch("X-Signature", "deadbeef", "/hmac", Some(SECRET)),
    )
    .unwrap();
    assert_eq!(reply.status, 401);
    assert!(events.is_empty());
}

#[test]
fn mismatched_signature_is_rejected() {
    let cfg = config(Algorithm::Sha256, Encoding::Hex, None);
    // correct length, correct encoding, wrong secret
    let forged = sign(&cfg, "wrong-secret", BODY);
    let (reply, events) = handle_hmac_dispatch(
        &cfg,
        dispatch("X-Signature", &forged, "/hmac", Some(SECRET)),
    )
    .unwrap();
    assert_eq!(reply.status, 401);
    assert!(events.is_empty());
}

#[test]
fn tampered_body_is_rejected() {
    let cfg = config(Algorithm::Sha256, Encoding::Hex, None);
    let sig = sign(&cfg, SECRET, BODY);
    // sign the real body, but deliver a different one
    let mut headers = BTreeMap::new();
    headers.insert("X-Signature".to_string(), sig);
    let msg = SrvMsg::Dispatch {
        req_id: uuid::Uuid::nil(),
        method: "POST".to_string(),
        path: "/hmac".to_string(),
        query: None,
        headers,
        body_b64: base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b"{\"tampered\":true}",
        ),
        secret: Some(SECRET.to_string()),
    };
    let (reply, events) = handle_hmac_dispatch(&cfg, msg).unwrap();
    assert_eq!(reply.status, 401);
    assert!(events.is_empty());
}
