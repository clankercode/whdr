//! Whole-system tests: real `whdr-server` binary, real fake-extension
//! children, real HTTP/WS/UDS clients. These are the M2–M5 exit criteria
//! from docs/PLAN.md, driven by the whdr-ext-fake misbehavior harness.

use std::path::PathBuf;
use std::time::Duration;

use whdr_test_support::{ServerBuilder, WsSubscriber, b64, ext_row, http_request, subscriber_row};

fn server_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_whdr-server"))
}

fn fake_ext_bin() -> PathBuf {
    // cargo builds examples alongside test binaries: target/debug/examples/.
    let mut path = server_bin();
    path.pop();
    path.push("examples");
    path.push("whdr-ext-fake");
    assert!(
        path.exists(),
        "whdr-ext-fake example not built at {}",
        path.display()
    );
    path
}

fn builder() -> ServerBuilder {
    ServerBuilder::new(server_bin(), fake_ext_bin()).unwrap()
}

// ---------------------------------------------------------------- M2 exits

#[tokio::test]
async fn out_of_order_results_correlate_to_their_requests() {
    let server = builder()
        .with_fake_ext("alpha", "out_of_order_pairs = true")
        .unwrap()
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    let (first, second) = tokio::join!(
        http_request(server.ingest_addr, "POST", "/alpha", b"payload-one"),
        async {
            // Ensure deterministic dispatch order: one, then two.
            tokio::time::sleep(Duration::from_millis(150)).await;
            http_request(server.ingest_addr, "POST", "/alpha", b"payload-two").await
        }
    );
    let (status_one, body_one) = first.unwrap();
    let (status_two, body_two) = second.unwrap();

    // The fake answers request two before request one; correlation by
    // req_id must still hand each caller its own body.
    assert_eq!(status_one, 200);
    assert_eq!(status_two, 200);
    assert_eq!(body_one, b64(b"payload-one"));
    assert_eq!(body_two, b64(b"payload-two"));
}

#[tokio::test]
async fn dispatch_timeout_yields_504_and_late_result_is_dropped() {
    let server = builder()
        .with_fake_ext("alpha", "reply_delay_ms = 900")
        .unwrap()
        .with_limits("dispatch_timeout_ms = 250\nhang_kill_threshold = 10")
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    let (status, _) = http_request(server.ingest_addr, "POST", "/alpha", b"slow")
        .await
        .unwrap();
    assert_eq!(status, 504);

    // Wait for the late Result to arrive after the 504 already went out.
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(
        server.logs().contains("late or unknown result dropped"),
        "expected late-result warning in logs:\n{}",
        server.logs()
    );
    // The extension is still Ready — a late result is not a protocol error.
    let ext = server.wait_ext_state("alpha", "Ready").await.unwrap();
    assert_eq!(ext["protocol_errors"], 0);
}

#[tokio::test]
async fn full_in_flight_window_returns_429() {
    let server = builder()
        .with_fake_ext("alpha", "reply_delay_ms = 1200")
        .unwrap()
        .with_limits("max_in_flight = 1\ndispatch_timeout_ms = 5000")
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    let (slow, rejected) = tokio::join!(
        http_request(server.ingest_addr, "POST", "/alpha", b"occupies-window"),
        async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            http_request(server.ingest_addr, "POST", "/alpha", b"overflow").await
        }
    );
    assert_eq!(slow.unwrap().0, 200);
    assert_eq!(rejected.unwrap().0, 429);
}

#[tokio::test]
async fn stdout_garbage_kills_ext_into_crashloop_failed() {
    let server = builder()
        .with_fake_ext("alpha", "garbage_on_start = 3")
        .unwrap()
        .with_limits(
            "max_protocol_errors = 3\ncrashloop_threshold = 3\ncrashloop_window_ms = 60000",
        )
        .start()
        .await
        .unwrap();

    // Every generation spews garbage and is killed; after the crashloop
    // threshold the supervisor stops retrying and surfaces Failed.
    let ext = server.wait_ext_state("alpha", "Failed").await.unwrap();
    let reason = ext["reason"].as_str().unwrap_or_default();
    assert!(
        reason.contains("crashloop") || reason.contains("exit"),
        "unexpected failure reason: {reason}"
    );
}

#[tokio::test]
async fn extension_that_never_registers_is_marked_failed() {
    let server = builder()
        .with_fake_ext("alpha", "register = false")
        .unwrap()
        .with_timeouts("register_ms = 300")
        .start()
        .await
        .unwrap();

    server.wait_ext_state("alpha", "Failed").await.unwrap();
    // And its route is gone: server-generated response, not an ext reply.
    let (status, _) = http_request(server.ingest_addr, "POST", "/alpha", b"x")
        .await
        .unwrap();
    assert_ne!(status, 200);
}

// ---------------------------------------------------------------- M3 exits

#[tokio::test]
async fn sighup_hot_adds_and_drains_extensions() {
    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    // Hot add: install beta, enable it, SIGHUP.
    server
        .install_fake_ext(&fake_ext_bin(), "beta", "")
        .unwrap();
    server.set_enabled(&["alpha", "beta"]).unwrap();
    server.sighup().unwrap();
    server.wait_ext_state("beta", "Ready").await.unwrap();
    let (status, _) = http_request(server.ingest_addr, "POST", "/beta", b"hello")
        .await
        .unwrap();
    assert_eq!(status, 200);

    // Drain removal: disable beta, SIGHUP, route disappears.
    server.set_enabled(&["alpha"]).unwrap();
    server.sighup().unwrap();
    server
        .wait_status(|status| ext_row(status, "beta").is_none())
        .await
        .unwrap();
    let (status, _) = http_request(server.ingest_addr, "POST", "/beta", b"gone")
        .await
        .unwrap();
    assert_eq!(status, 404);
    // Alpha kept serving throughout.
    let (status, _) = http_request(server.ingest_addr, "POST", "/alpha", b"still-here")
        .await
        .unwrap();
    assert_eq!(status, 200);
}

#[tokio::test]
async fn hung_extension_is_killed_and_respawned() {
    let server = builder()
        .with_fake_ext("alpha", "stall_after = 0")
        .unwrap()
        .with_limits("dispatch_timeout_ms = 200\nhang_kill_threshold = 2")
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    for _ in 0..2 {
        let (status, _) = http_request(server.ingest_addr, "POST", "/alpha", b"hang")
            .await
            .unwrap();
        assert_eq!(status, 504);
    }

    // Two consecutive timeouts crosses the threshold: kill, backoff, respawn.
    server
        .wait_status(|status| {
            ext_row(status, "alpha").is_some_and(|ext| {
                ext["state"] == "Ready" && ext["restarts"].as_u64().unwrap_or(0) >= 1
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn spoofed_channel_is_dropped_and_counted() {
    let server = builder()
        .with_fake_ext("alpha", r#"event_channel = "github.push""#)
        .unwrap()
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    let token = server.token_add("watcher").await.unwrap();
    let (mut watcher, _) = WsSubscriber::connect(server.sub_addr, &token)
        .await
        .unwrap();
    watcher.subscribe(&[">"]).await.unwrap();

    let (status, _) = http_request(server.ingest_addr, "POST", "/alpha", b"spoof")
        .await
        .unwrap();
    assert_eq!(status, 200);

    // The spoofed github.push event must never reach subscribers…
    watcher
        .expect_silence(Duration::from_millis(600))
        .await
        .unwrap();
    // …and the violation is counted while nothing counts as emitted.
    let status = server.status().await.unwrap();
    let ext = ext_row(&status, "alpha").unwrap();
    assert!(ext["namespace_violations"].as_u64().unwrap() >= 1);
    assert_eq!(ext["events_emitted"], 0);
}

// ---------------------------------------------------------------- M4 exits

#[tokio::test]
async fn overlapping_subscribers_receive_exactly_their_matches() {
    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .with_fake_ext("beta", "")
        .unwrap()
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();
    server.wait_ext_state("beta", "Ready").await.unwrap();

    let narrow_token = server.token_add("narrow").await.unwrap();
    let wide_token = server.token_add("wide").await.unwrap();
    let (mut narrow, welcome) = WsSubscriber::connect(server.sub_addr, &narrow_token)
        .await
        .unwrap();
    assert_eq!(welcome["name"], "narrow");
    let (mut wide, _) = WsSubscriber::connect(server.sub_addr, &wide_token)
        .await
        .unwrap();
    narrow.subscribe(&["alpha.>"]).await.unwrap();
    wide.subscribe(&[">"]).await.unwrap();

    let (status, _) = http_request(server.ingest_addr, "POST", "/alpha", b"to-both")
        .await
        .unwrap();
    assert_eq!(status, 200);

    let narrow_event = narrow.recv_event(Duration::from_secs(5)).await.unwrap();
    let wide_event = wide.recv_event(Duration::from_secs(5)).await.unwrap();
    assert_eq!(narrow_event["channel"], "alpha.echo");
    assert_eq!(narrow_event["payload_b64"], b64(b"to-both"));
    // Same fan-out, same server-stamped identity for every subscriber.
    assert_eq!(narrow_event["id"], wide_event["id"]);
    assert!(narrow_event["ts_ms"].as_u64().unwrap() > 0);

    let (status, _) = http_request(server.ingest_addr, "POST", "/beta", b"wide-only")
        .await
        .unwrap();
    assert_eq!(status, 200);
    let wide_event = wide.recv_event(Duration::from_secs(5)).await.unwrap();
    assert_eq!(wide_event["channel"], "beta.echo");
    narrow
        .expect_silence(Duration::from_millis(500))
        .await
        .unwrap();
}

#[tokio::test]
async fn bad_token_is_rejected_and_revoke_closes_live_connection() {
    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .start()
        .await
        .unwrap();

    assert_eq!(
        WsSubscriber::connect_expect_reject(server.sub_addr, "tok_bogus")
            .await
            .unwrap(),
        401
    );

    let token = server.token_add("project").await.unwrap();
    let (mut live, _) = WsSubscriber::connect(server.sub_addr, &token)
        .await
        .unwrap();
    live.subscribe(&["alpha.>"]).await.unwrap();

    server
        .control(whdr_proto::ControlRequest::TokenRevoke {
            name: "project".to_string(),
        })
        .await
        .unwrap();

    // The live connection gets a closing frame with reason=revoked.
    let closing = live.recv(Duration::from_secs(5)).await.unwrap();
    assert_eq!(closing["type"], "closing");
    assert_eq!(closing["reason"], "revoked");

    // And the old token no longer authenticates.
    assert_eq!(
        WsSubscriber::connect_expect_reject(server.sub_addr, &token)
            .await
            .unwrap(),
        401
    );
}

#[tokio::test]
async fn stalled_subscriber_drops_oldest_while_healthy_loses_nothing() {
    const EVENTS: usize = 20;
    const PAYLOAD_BYTES: usize = 256 * 1024;

    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .with_limits("sub_queue_len = 4")
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    let slow_token = server.token_add("slow").await.unwrap();
    let fast_token = server.token_add("fast").await.unwrap();
    let (mut slow, _) = WsSubscriber::connect(server.sub_addr, &slow_token)
        .await
        .unwrap();
    let (mut fast, _) = WsSubscriber::connect(server.sub_addr, &fast_token)
        .await
        .unwrap();
    slow.subscribe(&["alpha.>"]).await.unwrap();
    fast.subscribe(&["alpha.>"]).await.unwrap();

    // Fire large events; the fast client reads as they come, the slow
    // client reads nothing until the end.
    let fast_reader = tokio::spawn(async move {
        let mut got = Vec::new();
        for _ in 0..EVENTS {
            let event = fast.recv_event(Duration::from_secs(20)).await.unwrap();
            got.push(event["payload_b64"].as_str().unwrap().to_string());
        }
        // Hand the client back so the connection stays open while status
        // is inspected below.
        (fast, got)
    });

    for n in 0..EVENTS {
        let mut body = vec![b'a' + (n % 26) as u8; PAYLOAD_BYTES];
        // Tag the payload so ordering is observable after b64 round-trip.
        body[..8].copy_from_slice(format!("msg{n:05}").as_bytes());
        let (status, _) = http_request(server.ingest_addr, "POST", "/alpha", &body)
            .await
            .unwrap();
        assert_eq!(status, 200);
    }

    // The healthy consumer saw every event, in order.
    let (_fast, fast_got) = fast_reader.await.unwrap();
    assert_eq!(fast_got.len(), EVENTS);
    for (n, payload) in fast_got.iter().enumerate() {
        assert_eq!(decode_tag(payload), format!("msg{n:05}"));
    }

    // The stalled consumer accrued drops (visible in status)…
    let status = server
        .wait_status(|status| {
            subscriber_row(status, "slow")
                .is_some_and(|row| row["dropped"].as_u64().unwrap_or(0) > 0)
        })
        .await
        .unwrap();
    let slow_row = subscriber_row(&status, "slow").unwrap();
    let dropped = slow_row["dropped"].as_u64().unwrap();
    let fast_row = subscriber_row(&status, "fast").unwrap();
    assert_eq!(fast_row["dropped"].as_u64().unwrap(), 0);

    // …and when it finally reads, the queue tail is the NEWEST events:
    // drop-oldest keeps fresh data, so the last frame must be the final msg.
    let mut received = Vec::new();
    while let Ok(event) = slow.recv_event(Duration::from_secs(2)).await {
        received.push(decode_tag(event["payload_b64"].as_str().unwrap()));
    }
    assert!(!received.is_empty());
    assert!(
        received.len() < EVENTS,
        "slow subscriber should have lost events (dropped={dropped})"
    );
    assert_eq!(
        received.last().unwrap(),
        &format!("msg{:05}", EVENTS - 1),
        "drop-oldest must preserve the newest event; got tail {:?}",
        received.last()
    );
}

fn decode_tag(payload_b64: &str) -> String {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload_b64)
        .unwrap();
    String::from_utf8_lossy(&bytes[..8]).to_string()
}

// ---------------------------------------------------------------- M5 exits

#[tokio::test]
async fn tokens_survive_a_hard_crash_and_restart() {
    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .start()
        .await
        .unwrap();

    let keep = server.token_add("keeper").await.unwrap();
    let gone = server.token_add("goner").await.unwrap();
    server
        .control(whdr_proto::ControlRequest::TokenRevoke {
            name: "goner".to_string(),
        })
        .await
        .unwrap();

    // Hard kill (no graceful shutdown) — the store must already be durable.
    let server = server.kill_and_restart().await.unwrap();

    let (_client, welcome) = WsSubscriber::connect(server.sub_addr, &keep).await.unwrap();
    assert_eq!(welcome["name"], "keeper");
    assert_eq!(
        WsSubscriber::connect_expect_reject(server.sub_addr, &gone)
            .await
            .unwrap(),
        401
    );
}

// ---------------------------------------------------------- durable delivery

#[tokio::test]
async fn subscribe_with_replay_streams_stored_then_live() {
    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .with_delivery("")
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    // Emit two events while NO subscriber is connected: persisted seq 1,2.
    for body in [b"one".as_slice(), b"two".as_slice()] {
        let (status, _) = http_request(server.ingest_addr, "POST", "/alpha", body)
            .await
            .unwrap();
        assert_eq!(status, 200);
    }

    let token = server.token_add("p").await.unwrap();
    let (mut sub, _welcome) = WsSubscriber::connect(server.sub_addr, &token)
        .await
        .unwrap();
    sub.send_json(&serde_json::json!({
        "type": "subscribe", "patterns": ["alpha.>"], "replay": {"after_seq": 0}
    }))
    .await
    .unwrap();

    let ok = sub.recv(Duration::from_secs(5)).await.unwrap();
    assert_eq!(ok["type"], "ok");
    let e1 = sub.recv(Duration::from_secs(5)).await.unwrap();
    assert_eq!(e1["type"], "event");
    assert_eq!(e1["seq"], 1);
    let e2 = sub.recv(Duration::from_secs(5)).await.unwrap();
    assert_eq!(e2["seq"], 2);
    let replayed = sub.recv(Duration::from_secs(5)).await.unwrap();
    assert_eq!(replayed["type"], "replayed");
    assert_eq!(replayed["through_seq"], 2);

    // A third event now arrives live with the next seq.
    let (status, _) = http_request(server.ingest_addr, "POST", "/alpha", b"three")
        .await
        .unwrap();
    assert_eq!(status, 200);
    let e3 = sub.recv_event(Duration::from_secs(5)).await.unwrap();
    assert_eq!(e3["seq"], 3);
    // All ids distinct: dedup-by-id would be a no-op here.
    assert_ne!(e1["id"], e2["id"]);
    assert_ne!(e2["id"], e3["id"]);
}

#[tokio::test]
async fn replay_below_floor_signals_replay_gap() {
    // Size-cap to a single retained event with a fast prune cadence, so the
    // retained floor rises above the requested cursor.
    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .with_delivery("max_events = 1\nprune_interval_secs = 1")
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    for n in 0..5u8 {
        let (status, _) = http_request(server.ingest_addr, "POST", "/alpha", &[b'a' + n])
            .await
            .unwrap();
        assert_eq!(status, 200);
    }

    let token = server.token_add("p").await.unwrap();
    // Poll: once the background prune has run, a replay from before the floor
    // yields an explicit replay_gap. Retry until the prune has fired.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let (mut probe, _) = WsSubscriber::connect(server.sub_addr, &token)
            .await
            .unwrap();
        probe
            .send_json(&serde_json::json!({
                "type": "subscribe", "patterns": ["alpha.>"], "replay": {"after_seq": 1}
            }))
            .await
            .unwrap();
        let ok = probe.recv(Duration::from_secs(5)).await.unwrap();
        assert_eq!(ok["type"], "ok");
        let next = probe.recv(Duration::from_secs(5)).await.unwrap();
        if next["type"] == "replay_gap" {
            assert_eq!(next["from_seq"], 1);
            assert_eq!(next["earliest_seq"], 5); // only seq 5 survives max_events=1
            // Replay then continues from the floor, then replayed.
            let ev = probe.recv(Duration::from_secs(5)).await.unwrap();
            assert_eq!(ev["type"], "event");
            assert_eq!(ev["seq"], 5);
            let replayed = probe.recv(Duration::from_secs(5)).await.unwrap();
            assert_eq!(replayed["type"], "replayed");
            assert_eq!(replayed["through_seq"], 5);
            break;
        }
        // Prune has not fired yet; the full window replayed. Retry.
        assert!(
            tokio::time::Instant::now() < deadline,
            "background prune never raised the floor; last frame {next}"
        );
        drop(probe);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

#[tokio::test]
async fn replay_refused_when_delivery_disabled() {
    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    let token = server.token_add("p").await.unwrap();
    let (mut sub, _welcome) = WsSubscriber::connect(server.sub_addr, &token)
        .await
        .unwrap();
    sub.send_json(&serde_json::json!({
        "type": "subscribe", "patterns": ["alpha.>"], "replay": {"after_seq": 0}
    }))
    .await
    .unwrap();

    // Live subscription still succeeds; replay is refused with an error.
    let ok = sub.recv(Duration::from_secs(5)).await.unwrap();
    assert_eq!(ok["type"], "ok");
    assert_eq!(ok["op"], "subscribe");
    let err = sub.recv(Duration::from_secs(5)).await.unwrap();
    assert_eq!(err["type"], "error");
    assert_eq!(err["op"], "replay");
    assert!(
        err["msg"].as_str().unwrap().contains("not enabled"),
        "unexpected replay error msg: {err}"
    );
}

#[tokio::test]
async fn slow_consumer_gets_lagged_then_replays_missed_events() {
    const EVENTS: u64 = 12;
    const PAYLOAD_BYTES: usize = 256 * 1024;

    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .with_delivery("")
        .with_limits("sub_queue_len = 1")
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    let token = server.token_add("slow").await.unwrap();
    let (mut slow, _welcome) = WsSubscriber::connect(server.sub_addr, &token)
        .await
        .unwrap();
    slow.subscribe(&["alpha.>"]).await.unwrap();

    // Fire a burst the slow client won't read: the server evicts oldest.
    for n in 0..EVENTS {
        let body = vec![b'a' + (n % 26) as u8; PAYLOAD_BYTES];
        let (status, _) = http_request(server.ingest_addr, "POST", "/alpha", &body)
            .await
            .unwrap();
        assert_eq!(status, 200);
    }

    // The client reads until it observes an explicit `lagged`. Its cursor is
    // the highest seq processed *before* the drop notice.
    let mut cursor = 0u64;
    loop {
        let frame = slow.recv(Duration::from_secs(10)).await.unwrap();
        match frame["type"].as_str() {
            Some("lagged") => {
                assert!(frame["dropped"].as_u64().unwrap() > 0, "lagged dropped>0");
                break;
            }
            Some("event") => cursor = cursor.max(frame["seq"].as_u64().unwrap()),
            _ => {}
        }
    }
    drop(slow);

    // Reconnect and replay from the cursor: every missed seq up to head is
    // delivered — the drop was recoverable, not permanent loss.
    let (mut recovered, _welcome) = WsSubscriber::connect(server.sub_addr, &token)
        .await
        .unwrap();
    recovered
        .send_json(&serde_json::json!({
            "type": "subscribe", "patterns": ["alpha.>"], "replay": {"after_seq": cursor}
        }))
        .await
        .unwrap();
    let ok = recovered.recv(Duration::from_secs(5)).await.unwrap();
    assert_eq!(ok["type"], "ok");

    let mut replayed_seqs = Vec::new();
    loop {
        let frame = recovered.recv(Duration::from_secs(5)).await.unwrap();
        match frame["type"].as_str() {
            Some("event") => replayed_seqs.push(frame["seq"].as_u64().unwrap()),
            Some("replayed") => {
                assert_eq!(frame["through_seq"].as_u64().unwrap(), EVENTS);
                break;
            }
            _ => {}
        }
    }
    let expected: Vec<u64> = ((cursor + 1)..=EVENTS).collect();
    assert_eq!(replayed_seqs, expected, "replay must cover the whole gap");
}

#[tokio::test]
async fn stale_tmp_file_does_not_break_the_token_store() {
    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .start()
        .await
        .unwrap();
    let keep = server.token_add("keeper").await.unwrap();

    // Simulate a crash between tmp-write and rename: a truncated tmp file
    // sits next to the store. The store itself must still load.
    let tmp = server.config_path.parent().unwrap().join("tokens.toml.tmp");
    std::fs::write(&tmp, "[garbage\n").unwrap();

    let server = server.kill_and_restart().await.unwrap();
    let (_client, welcome) = WsSubscriber::connect(server.sub_addr, &keep).await.unwrap();
    assert_eq!(welcome["name"], "keeper");
}
