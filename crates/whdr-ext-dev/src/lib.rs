use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use whdr_ext_kit::{decode_body_b64, text_reply};
use whdr_proto::{Event, HttpReply, SrvMsg};

pub fn handle_dev_dispatch(msg: SrvMsg) -> Result<(HttpReply, Vec<Event>)> {
    let SrvMsg::Dispatch { body_b64, .. } = msg else {
        return Ok((text_reply(200, "shutdown ignored"), vec![]));
    };
    let body = decode_body_b64(&body_b64)?;
    Ok((
        text_reply(200, String::from_utf8_lossy(&body).to_string()),
        vec![Event {
            channel: "dev.echo".to_string(),
            payload_b64: STANDARD.encode(body),
        }],
    ))
}
