//! Subscriber WebSocket plane: bearer-token handshake, per-connection
//! outbound queue, app-level subscribe/unsubscribe/ping, and WS-ping
//! liveness (SPEC §9).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::body::Bytes;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::watch;
use tokio::time;
use tracing::warn;
use whdr_proto::{ClosingReason, Pattern, SubClientMsg, SubServerMsg};

use crate::daemon::AppState;
use crate::outbound_queue::OutboundQueue;
use crate::subscribers::SubscriberRegistration;

pub(crate) async fn subscribe_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let token = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_string);
    let Some(token) = token else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Some(name) = state.authenticate_subscriber(&token).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    ws.on_upgrade(move |socket| subscriber_socket(state, name, socket))
        .into_response()
}

async fn subscriber_socket(state: AppState, name: String, socket: WebSocket) {
    let config = state.config().await;
    let ws_idle_timeout = Duration::from_millis(config.subscribers.ws_idle_timeout_ms.max(1));
    let queue = Arc::new(OutboundQueue::new(
        config.limits.sub_queue_len,
        config.limits.sub_queue_bytes,
    ));
    let (close_tx, mut close_rx) = watch::channel::<Option<ClosingReason>>(None);
    let id = state
        .subscribers()
        .insert(SubscriberRegistration {
            name: name.clone(),
            remote_addr: None,
            queue: queue.clone(),
            close_tx,
        })
        .await;

    let (mut sink, mut stream) = socket.split();
    let _ = sink
        .send(Message::Text(
            serde_json::to_string(&SubServerMsg::Welcome { name })
                .unwrap()
                .into(),
        ))
        .await;

    let mut ping_interval = time::interval(ws_idle_timeout);
    ping_interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    ping_interval.tick().await;
    let mut awaiting_pong = false;

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                if awaiting_pong {
                    warn!(subscriber = id, "subscriber missed websocket pong; closing");
                    break;
                }
                if sink.send(Message::Ping(Bytes::new())).await.is_err() {
                    break;
                }
                awaiting_pong = true;
            }
            frame = queue.pop() => {
                // Surface any coalesced slow-consumer drops as a single
                // `lagged` frame ahead of the next event ([D-lag]); the client
                // reconnects and replays from its cursor to recover.
                if let Some(dropped) = queue.take_pending_lag() {
                    let lagged = encode(&SubServerMsg::Lagged {
                        dropped: dropped as u64,
                    });
                    if sink.send(Message::Text(lagged.into())).await.is_err() {
                        break;
                    }
                }
                if sink.send(Message::Text(frame.text.to_string().into())).await.is_err() {
                    break;
                }
                if frame.closing {
                    break;
                }
            }
            changed = close_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let Some(reason) = close_rx.borrow().clone() else {
                    continue;
                };
                if let Ok(text) = serde_json::to_string(&SubServerMsg::Closing { reason }) {
                    let _ = sink.send(Message::Text(text.into())).await;
                }
                break;
            }
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        match handle_subscriber_text(&state, id, text.as_str(), &queue).await {
                            Ok(replay_frames) => {
                                // Replay is streamed directly to the sink so a
                                // large window can't self-evict from the bounded
                                // live queue (§9.4 [D-dedup]).
                                let mut send_failed = false;
                                for frame in replay_frames {
                                    if sink.send(Message::Text(frame.into())).await.is_err() {
                                        send_failed = true;
                                        break;
                                    }
                                }
                                if send_failed {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        let _ = sink.send(Message::Pong(payload)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        awaiting_pong = false;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        }
    }

    state.subscribers().remove(id).await;
}

/// Handle one client frame. Returns the serialized frames that must be
/// streamed **directly to the sink** (in order): only the replay phase uses
/// this, so a large replay window bypasses the bounded live queue. Control
/// acks for the ordinary path still go through the queue.
async fn handle_subscriber_text(
    state: &AppState,
    id: u64,
    text: &str,
    queue: &OutboundQueue,
) -> Result<Vec<String>> {
    let msg: SubClientMsg = serde_json::from_str(text)?;
    match msg {
        SubClientMsg::Subscribe { patterns, replay } => {
            if let Err(msg) = state.subscribers().subscribe(id, patterns.clone()).await {
                queue.push_control(&SubServerMsg::Error {
                    op: "subscribe".to_string(),
                    msg,
                });
                return Ok(Vec::new());
            }
            let Some(replay) = replay else {
                queue.push_control(&SubServerMsg::Ok {
                    op: "subscribe".to_string(),
                });
                return Ok(Vec::new());
            };
            let Some(log) = state.delivery() else {
                // Live subscription is active; replay is simply refused.
                queue.push_control(&SubServerMsg::Ok {
                    op: "subscribe".to_string(),
                });
                queue.push_control(&SubServerMsg::Error {
                    op: "replay".to_string(),
                    msg: "durable delivery is not enabled".to_string(),
                });
                return Ok(Vec::new());
            };

            // Replay is streamed directly to the sink, so the ok ack leads the
            // window (it cannot go through the queue or it might trail the
            // direct frames).
            let mut frames = vec![encode(&SubServerMsg::Ok {
                op: "subscribe".to_string(),
            })];
            let parsed: Vec<Pattern> = patterns
                .iter()
                .filter_map(|pattern| Pattern::new(pattern.clone()).ok())
                .collect();

            let floor = log.floor_seq();
            let mut effective_after = replay.after_seq;
            if floor != 0 && replay.after_seq + 1 < floor {
                // Requested cursor predates the retained floor: explicit gap.
                frames.push(encode(&SubServerMsg::ReplayGap {
                    from_seq: replay.after_seq,
                    earliest_seq: floor,
                }));
                effective_after = floor - 1;
            }

            let head = log.head_seq();
            for row in log.read_after(effective_after, Some(&parsed))? {
                if row.seq > head {
                    break; // bound the window at the head snapshot; live follows
                }
                frames.push(encode(&SubServerMsg::Event {
                    id: row.id,
                    seq: row.seq,
                    ts_ms: row.ts_ms,
                    channel: row.channel,
                    payload_b64: row.payload_b64,
                }));
            }
            frames.push(encode(&SubServerMsg::Replayed { through_seq: head }));
            return Ok(frames);
        }
        SubClientMsg::Unsubscribe { patterns } => {
            state.subscribers().unsubscribe(id, &patterns).await;
            queue.push_control(&SubServerMsg::Ok {
                op: "unsubscribe".to_string(),
            });
        }
        SubClientMsg::Ping => {
            queue.push_control(&SubServerMsg::Pong);
        }
    }
    Ok(Vec::new())
}

fn encode(msg: &SubServerMsg) -> String {
    serde_json::to_string(msg).unwrap_or_default()
}
