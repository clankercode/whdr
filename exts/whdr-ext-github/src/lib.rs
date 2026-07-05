use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde_json::Value;
use whdr_ext_kit::{decode_body_b64, header_value, hmac_sha256_hex, text_reply};
use whdr_proto::{Event, HttpReply, SrvMsg};

pub fn github_signature(secret: &str, body: &[u8]) -> String {
    format!("sha256={}", hmac_sha256_hex(secret, body))
}

pub fn handle_github_dispatch(msg: SrvMsg) -> Result<(HttpReply, Vec<Event>)> {
    let SrvMsg::Dispatch {
        headers,
        body_b64,
        secret,
        ..
    } = msg
    else {
        return Ok((text_reply(200, "shutdown ignored"), vec![]));
    };

    let body = decode_body_b64(&body_b64)?;
    let Some(secret) = secret else {
        return Ok((text_reply(401, "missing secret"), vec![]));
    };
    let expected = github_signature(&secret, &body);
    let provided = header_value(&headers, "x-hub-signature-256").unwrap_or("");
    if provided != expected {
        return Ok((text_reply(401, "invalid signature"), vec![]));
    }

    let github_event = header_value(&headers, "x-github-event").unwrap_or("unknown");
    let parsed: Value = serde_json::from_slice(&body).context("parse github webhook json")?;
    let action = parsed
        .get("action")
        .and_then(Value::as_str)
        .map(channel_token);
    let mut channel = format!("github.{}", channel_token(github_event));
    if let Some(action) = action
        && !action.is_empty()
    {
        channel.push('.');
        channel.push_str(&action);
    }

    Ok((
        text_reply(200, "ok"),
        vec![Event {
            channel,
            payload_b64: STANDARD.encode(body),
        }],
    ))
}

fn channel_token(input: &str) -> String {
    let mut out = String::new();
    for ch in input.chars() {
        match ch {
            'a'..='z' | '0'..='9' | '_' | '-' => out.push(ch),
            'A'..='Z' => out.push(ch.to_ascii_lowercase()),
            _ => out.push('-'),
        }
    }
    let trimmed = out.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "unknown".to_string()
    } else {
        trimmed
    }
}
