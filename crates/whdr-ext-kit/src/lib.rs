use std::collections::BTreeMap;

use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use whdr_proto::{Event, ExtMsg, HttpReply, PROTOCOL_VERSION, SrvMsg, decode_line, encode_line};

pub type DispatchResult = Result<(HttpReply, Vec<Event>)>;

#[async_trait]
pub trait Extension: Send + Sync + 'static {
    async fn handle_dispatch(&self, dispatch: SrvMsg) -> DispatchResult;
}

pub async fn run_extension<E: Extension>(
    id: &str,
    paths: Vec<String>,
    channels: Vec<String>,
    meta: serde_json::Value,
    extension: E,
) -> Result<()> {
    let stdout = tokio::io::stdout();
    let mut writer = BufWriter::new(stdout);
    let register = ExtMsg::Register {
        protocol: PROTOCOL_VERSION,
        id: Some(id.to_string()),
        paths,
        channels,
        meta,
    };
    writer.write_all(encode_line(&register)?.as_bytes()).await?;
    writer.flush().await?;

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    while let Some(line) = lines.next_line().await? {
        let Some(msg) = decode_line::<SrvMsg>(&line)? else {
            continue;
        };
        match msg {
            SrvMsg::Dispatch { req_id, .. } => {
                let (http, events) = extension.handle_dispatch(msg).await?;
                let response = ExtMsg::Result {
                    req_id,
                    http,
                    events,
                };
                writer.write_all(encode_line(&response)?.as_bytes()).await?;
                writer.flush().await?;
            }
            SrvMsg::Shutdown => break,
        }
    }

    Ok(())
}

pub fn decode_body_b64(body_b64: &str) -> Result<Vec<u8>> {
    STANDARD
        .decode(body_b64)
        .context("dispatch body is not valid base64")
}

pub fn text_reply(status: u16, body: impl Into<String>) -> HttpReply {
    HttpReply {
        status,
        headers: BTreeMap::new(),
        body: body.into(),
    }
}

pub fn header_value<'a>(headers: &'a BTreeMap<String, String>, name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

pub fn hmac_sha256_hex(secret: &str, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any size");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}
