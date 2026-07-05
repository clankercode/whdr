use std::collections::BTreeMap;

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde_json::Value;
use subtle::ConstantTimeEq;
use whdr_ext_kit::{decode_body_b64, text_reply};
use whdr_proto::{Event, HttpReply, SrvMsg};

pub fn handle_teams_dispatch(msg: SrvMsg) -> Result<(HttpReply, Vec<Event>)> {
    let SrvMsg::Dispatch {
        method,
        query,
        body_b64,
        secret,
        ..
    } = msg
    else {
        return Ok((text_reply(200, "shutdown ignored"), vec![]));
    };

    if (method.eq_ignore_ascii_case("GET") || method.eq_ignore_ascii_case("POST"))
        && let Some(token) = validation_token(query.as_deref())
    {
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), "text/plain".to_string());
        return Ok((
            HttpReply {
                status: 200,
                headers,
                body: token,
            },
            vec![],
        ));
    }

    let body = decode_body_b64(&body_b64)?;
    let parsed: Value = serde_json::from_slice(&body).context("parse teams notification json")?;
    let Some(secret) = secret else {
        return Ok((text_reply(401, "missing clientState secret"), vec![]));
    };
    let mut events = Vec::new();
    if let Some(values) = parsed.get("value").and_then(Value::as_array) {
        for item in values {
            let client_state = item
                .get("clientState")
                .and_then(Value::as_str)
                .unwrap_or("");
            if client_state.as_bytes().ct_eq(secret.as_bytes()).unwrap_u8() != 1 {
                return Ok((text_reply(401, "invalid clientState"), vec![]));
            }
            if let Some(resource) = item.get("resource").and_then(Value::as_str) {
                events.push(Event {
                    channel: format!("teams.{}", resource_to_channel(resource)),
                    payload_b64: STANDARD.encode(&body),
                });
            }
        }
    }
    Ok((text_reply(202, "accepted"), events))
}

fn validation_token(query: Option<&str>) -> Option<String> {
    let query = query?;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key == "validationToken" {
            return Some(percent_decode(value));
        }
    }
    None
}

fn percent_decode(value: &str) -> String {
    let mut out = String::new();
    let bytes = value.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'+' {
            out.push(' ');
            i += 1;
        } else if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = u8::from_str_radix(&value[i + 1..i + 3], 16) {
                out.push(hex as char);
                i += 3;
            } else {
                out.push('%');
                i += 1;
            }
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

fn resource_to_channel(resource: &str) -> String {
    resource
        .chars()
        .map(|ch| match ch {
            'a'..='z' | '0'..='9' | '_' | '-' => ch,
            'A'..='Z' => ch.to_ascii_lowercase(),
            '/' | '.' | ':' => '.',
            _ => '-',
        })
        .collect::<String>()
        .trim_matches('.')
        .replace("..", ".")
}
