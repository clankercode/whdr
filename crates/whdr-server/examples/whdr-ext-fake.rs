//! Scriptable fake extension — the misbehavior harness the test plan (M1)
//! leans on. System tests copy this binary into a temp PATH dir as
//! `whdr-ext-<id>` and drop a `whdr-ext-<id>.toml` behavior file next to it;
//! the binary reads the file named after its own argv[0], so parallel tests
//! never share state through the environment.
//!
//! With no behavior file it acts as a well-behaved echo extension.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use whdr_proto::{Event, ExtMsg, HttpReply, PROTOCOL_VERSION, SrvMsg, decode_line, encode_line};

#[derive(Debug, Deserialize)]
#[serde(default)]
struct Behavior {
    /// Send Register at startup. `false` reproduces the never-registers ext.
    register: bool,
    register_delay_ms: u64,
    /// Exit(1) immediately after start — crashloop fuel.
    exit_on_start: bool,
    id: Option<String>,
    paths: Vec<String>,
    channels: Vec<String>,
    protocol: u32,
    /// Non-JSON lines written to stdout right after register (protocol errors).
    garbage_on_start: usize,
    /// Delay before every Result.
    reply_delay_ms: u64,
    /// Hold each odd dispatch and answer it after the following one: replies
    /// arrive out of order relative to requests.
    out_of_order_pairs: bool,
    /// Stop replying (but keep reading) after N results. 0 = never reply.
    stall_after: Option<usize>,
    /// Exit(1) after N results.
    exit_after: Option<usize>,
    /// When exiting via exit_after, first write half a JSON line.
    die_mid_line: bool,
    events_per_dispatch: usize,
    /// Channel for emitted events; defaults to `<id>.echo`. Point it at a
    /// foreign namespace to exercise spoof rejection.
    event_channel: Option<String>,
    /// Unsolicited events pushed right after register.
    flood_events: usize,
    status: u16,
}

impl Default for Behavior {
    fn default() -> Self {
        Self {
            register: true,
            register_delay_ms: 0,
            exit_on_start: false,
            id: None,
            paths: Vec::new(),
            channels: Vec::new(),
            protocol: PROTOCOL_VERSION,
            garbage_on_start: 0,
            reply_delay_ms: 0,
            out_of_order_pairs: false,
            stall_after: None,
            exit_after: None,
            die_mid_line: false,
            events_per_dispatch: 1,
            event_channel: None,
            flood_events: 0,
            status: 200,
        }
    }
}

fn load_behavior() -> (String, Behavior) {
    let argv0 = std::env::args().next().unwrap_or_default();
    let path = PathBuf::from(&argv0);
    let ext_id = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("whdr-ext-"))
        .unwrap_or("fake")
        .to_string();
    let behavior_path = PathBuf::from(format!("{argv0}.toml"));
    let behavior = std::fs::read_to_string(&behavior_path)
        .ok()
        .and_then(|text| toml::from_str(&text).ok())
        .unwrap_or_default();
    (ext_id, behavior)
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let (ext_id, behavior) = load_behavior();
    if behavior.exit_on_start {
        std::process::exit(1);
    }

    let mut stdout = tokio::io::stdout();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    if behavior.register_delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(behavior.register_delay_ms)).await;
    }
    if !behavior.register {
        // Read forever without registering; the server must kill us.
        while lines.next_line().await.ok().flatten().is_some() {}
        return;
    }

    let register = ExtMsg::Register {
        protocol: behavior.protocol,
        id: behavior.id.clone(),
        paths: behavior.paths.clone(),
        channels: behavior.channels.clone(),
        meta: serde_json::json!({"fake": true}),
    };
    write_msg(&mut stdout, &register).await;

    for n in 0..behavior.garbage_on_start {
        let _ = stdout
            .write_all(format!("this is not json {n}\n").as_bytes())
            .await;
        let _ = stdout.flush().await;
    }

    let channel = behavior
        .event_channel
        .clone()
        .unwrap_or_else(|| format!("{ext_id}.echo"));

    for n in 0..behavior.flood_events {
        let event = ExtMsg::Event {
            ev: Event {
                channel: channel.clone(),
                payload_b64: base64_encode(format!("flood-{n}").as_bytes()),
            },
        };
        write_msg(&mut stdout, &event).await;
    }

    let mut replies_sent = 0usize;
    let mut held: VecDeque<(uuid::Uuid, String)> = VecDeque::new();

    while let Ok(Some(line)) = lines.next_line().await {
        let msg = match decode_line::<SrvMsg>(&line) {
            Ok(Some(msg)) => msg,
            Ok(None) => continue,
            Err(_) => continue,
        };
        match msg {
            SrvMsg::Shutdown => return,
            SrvMsg::Dispatch {
                req_id, body_b64, ..
            } => {
                if behavior.stall_after.is_some_and(|max| replies_sent >= max) {
                    continue;
                }
                if behavior.out_of_order_pairs {
                    held.push_back((req_id, body_b64));
                    if held.len() < 2 {
                        continue;
                    }
                    // Answer the newest first, then the held one: out of order.
                    while let Some((id, body)) = held.pop_back() {
                        reply(
                            &mut stdout,
                            &behavior,
                            &channel,
                            id,
                            body,
                            &mut replies_sent,
                        )
                        .await;
                    }
                } else {
                    reply(
                        &mut stdout,
                        &behavior,
                        &channel,
                        req_id,
                        body_b64,
                        &mut replies_sent,
                    )
                    .await;
                }
                if behavior.exit_after.is_some_and(|max| replies_sent >= max) {
                    if behavior.die_mid_line {
                        let _ = stdout.write_all(br#"{"type":"result","req_id":"#).await;
                        let _ = stdout.flush().await;
                    }
                    std::process::exit(1);
                }
            }
        }
    }
}

async fn reply(
    stdout: &mut tokio::io::Stdout,
    behavior: &Behavior,
    channel: &str,
    req_id: uuid::Uuid,
    body_b64: String,
    replies_sent: &mut usize,
) {
    if behavior.reply_delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(behavior.reply_delay_ms)).await;
    }
    let events = (0..behavior.events_per_dispatch)
        .map(|_| Event {
            channel: channel.to_string(),
            payload_b64: body_b64.clone(),
        })
        .collect();
    let result = ExtMsg::Result {
        req_id,
        http: HttpReply {
            status: behavior.status,
            headers: Default::default(),
            body: body_b64,
        },
        events,
    };
    write_msg(stdout, &result).await;
    *replies_sent += 1;
}

async fn write_msg(stdout: &mut tokio::io::Stdout, msg: &ExtMsg) {
    if let Ok(line) = encode_line(msg) {
        let _ = stdout.write_all(line.as_bytes()).await;
        let _ = stdout.flush().await;
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}
