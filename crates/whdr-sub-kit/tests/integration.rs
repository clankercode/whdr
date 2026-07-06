//! Integration tests: the real `whdr-sub-kit` client against a real
//! `whdr-server` booted via `whdr-test-support`. These exercise the wire
//! protocol end-to-end (live subscribe, resume-after-disconnect exactly-once,
//! `replay_gap`, durability-disabled path, `revoked` → fatal, `lagged`
//! recovery).
//!
//! Because Cargo only exposes `CARGO_BIN_EXE_*` inside the crate that owns the
//! binary, we locate — and build if absent — the `whdr-server` binary and the
//! `whdr-ext-fake` example ourselves from the profile target dir.

use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use whdr_proto::ControlRequest;
use whdr_sub_kit::{
    Client, Connection, DeliveredEvent, Error, Handler, MemoryCursorStore, ResumeState,
    SubServerMsg,
};
use whdr_test_support::{ServerBuilder, ServerHandle, http_request};

/// Locate the profile target dir (e.g. `target/debug`) from the test binary.
fn profile_dir() -> PathBuf {
    let mut exe = std::env::current_exe().expect("current_exe");
    exe.pop(); // integration-<hash>
    if exe.file_name().and_then(|n| n.to_str()) == Some("deps") {
        exe.pop(); // target/<profile>
    }
    exe
}

/// Build (once) and return the `whdr-server` binary + `whdr-ext-fake` example
/// paths. Building from within the test is safe: the outer `cargo test` has
/// already released the build lock by the time tests run.
fn binaries() -> &'static (PathBuf, PathBuf) {
    static PATHS: OnceLock<(PathBuf, PathBuf)> = OnceLock::new();
    PATHS.get_or_init(|| {
        let dir = profile_dir();
        let server = dir.join("whdr-server");
        let fake = dir.join("examples").join("whdr-ext-fake");
        if !server.exists() || !fake.exists() {
            let status = Command::new(env!("CARGO"))
                .args([
                    "build",
                    "-p",
                    "whdr-server",
                    "--bin",
                    "whdr-server",
                    "--example",
                    "whdr-ext-fake",
                    "--jobs",
                    "2",
                ])
                .status()
                .expect("spawn cargo build for whdr-server");
            assert!(status.success(), "failed to build whdr-server + fake ext");
        }
        assert!(
            server.exists(),
            "whdr-server missing at {}",
            server.display()
        );
        assert!(fake.exists(), "whdr-ext-fake missing at {}", fake.display());
        (server, fake)
    })
}

fn builder() -> ServerBuilder {
    let (server, fake) = binaries();
    ServerBuilder::new(server.clone(), fake.clone()).unwrap()
}

fn sub_url(server: &ServerHandle) -> String {
    format!("ws://{}/subscribe", server.sub_addr)
}

async fn post(server: &ServerHandle, body: &[u8]) {
    let (status, _) = http_request(server.ingest_addr, "POST", "/alpha", body)
        .await
        .unwrap();
    assert_eq!(status, 200, "ingest POST should succeed");
}

// ---- conformance item 1: bad token → 401 → fatal Auth ---------------------

#[tokio::test]
async fn bad_token_is_fatal_auth_error() {
    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    let err = match Connection::connect(&sub_url(&server), "tok_bogus").await {
        Ok(_) => panic!("bogus token should be rejected"),
        Err(err) => err,
    };
    assert!(matches!(err, Error::Auth), "expected Auth, got {err:?}");
    assert!(err.is_fatal());
}

// ---- conformance items 1,2 + durability-disabled path ---------------------

#[tokio::test]
async fn live_subscribe_works_and_replay_is_refused_when_disabled() {
    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    let token = server.token_add("p").await.unwrap();
    // Client::connect subscribes with replay.after_seq = cursor (0). With
    // durability off, the server refuses replay but the live subscription
    // still works — the kit keeps functioning.
    let client = Client::builder(sub_url(&server), &token)
        .pattern("alpha.>")
        .build();
    let mut conn = client.connect().await.unwrap();
    assert_eq!(conn.name(), "p", "welcome echoes the token label");

    post(&server, b"hello").await;

    let mut saw_replay_refused = false;
    let mut event: Option<DeliveredEvent> = None;
    for _ in 0..10 {
        match conn.recv().await.unwrap() {
            SubServerMsg::Error { op, msg } if op == "replay" => {
                assert!(msg.contains("not enabled") || msg.to_lowercase().contains("disabled"));
                saw_replay_refused = true;
            }
            SubServerMsg::Event {
                id,
                seq,
                ts_ms,
                channel,
                payload_b64,
            } => {
                event = Some(DeliveredEvent {
                    id,
                    seq,
                    ts_ms,
                    channel,
                    payload_b64,
                });
                break;
            }
            _ => {}
        }
    }
    let event = event.expect("received a live event");
    assert_eq!(event.channel, "alpha.echo");
    assert_eq!(event.payload().unwrap(), b"hello");
    assert!(saw_replay_refused, "replay refused error surfaced");
}

// ---- conformance items 3,4,5 + resume-after-disconnect exactly-once -------

#[tokio::test]
async fn resume_after_disconnect_replays_missed_exactly_once() {
    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .with_delivery("")
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    let token = server.token_add("p").await.unwrap();
    let mut resume = ResumeState::new(0, 8192);
    let mut processed: Vec<uuid::Uuid> = Vec::new();

    // Session 1: subscribe live from cursor 0, receive one event.
    let mut conn = Connection::connect(&sub_url(&server), &token)
        .await
        .unwrap();
    conn.subscribe(&["alpha.>".to_string()], Some(resume.cursor()))
        .await
        .unwrap();
    post(&server, b"one").await;
    loop {
        if let SubServerMsg::Event { id, seq, .. } = conn.recv().await.unwrap() {
            if resume.should_process(id, seq) {
                processed.push(id);
                resume.record(id, seq);
            }
            break;
        }
    }
    assert_eq!(resume.cursor(), 1, "processed seq 1 live");
    drop(conn); // disconnect

    // While disconnected, two more events are persisted (seq 2, 3).
    post(&server, b"two").await;
    post(&server, b"three").await;

    // Session 2: resume from the cursor. The server replays seq 2,3 (not the
    // already-seen seq 1), then `replayed`.
    let mut conn = Connection::connect(&sub_url(&server), &token)
        .await
        .unwrap();
    conn.subscribe(&["alpha.>".to_string()], Some(resume.cursor()))
        .await
        .unwrap();
    let mut replayed_through = None;
    for _ in 0..20 {
        match conn.recv().await.unwrap() {
            SubServerMsg::Event { id, seq, .. } => {
                // Exactly-once: seq 1 must never be handed to us again.
                assert!(seq > 1, "replay must not re-deliver below the cursor");
                if resume.should_process(id, seq) {
                    processed.push(id);
                    resume.record(id, seq);
                }
            }
            SubServerMsg::Replayed { through_seq } => {
                replayed_through = Some(through_seq);
                break;
            }
            _ => {}
        }
    }
    assert_eq!(replayed_through, Some(3), "caught up through seq 3");
    assert_eq!(resume.cursor(), 3);

    // Exactly-once at the handler: three distinct events, no duplicates.
    processed.sort();
    processed.dedup();
    assert_eq!(processed.len(), 3, "each event handled exactly once");
}

// ---- conformance item 7: replay_gap surfaced ------------------------------

#[tokio::test]
async fn replay_gap_is_surfaced_explicitly() {
    // Retain only the newest event; a resume from an older cursor must yield
    // an explicit replay_gap rather than silent loss.
    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .with_delivery("max_events = 1\nprune_interval_secs = 1")
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    for _ in 0..5u8 {
        post(&server, b"x").await;
    }

    let token = server.token_add("p").await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut conn = Connection::connect(&sub_url(&server), &token)
            .await
            .unwrap();
        conn.subscribe(&["alpha.>".to_string()], Some(1))
            .await
            .unwrap();
        // Skip the `ok`, look at the next frame.
        let mut frame = conn.recv().await.unwrap();
        if matches!(frame, SubServerMsg::Ok { .. }) {
            frame = conn.recv().await.unwrap();
        }
        if let SubServerMsg::ReplayGap {
            from_seq,
            earliest_seq,
        } = frame
        {
            assert_eq!(from_seq, 1);
            assert_eq!(earliest_seq, 5, "only seq 5 survived max_events=1");
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "background prune never raised the floor; frame {frame:?}"
        );
        drop(conn);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

// ---- conformance items 6,8: revoked → fatal via run() ---------------------

struct Collector {
    seen: Arc<Mutex<Vec<u64>>>,
    delay: Duration,
}

#[async_trait::async_trait]
impl Handler for Collector {
    async fn on_event(&mut self, event: &DeliveredEvent) -> anyhow::Result<()> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.seen.lock().unwrap().push(event.seq);
        Ok(())
    }
}

#[tokio::test]
async fn revoked_token_stops_run_with_fatal_error() {
    let server = builder()
        .with_fake_ext("alpha", "")
        .unwrap()
        .start()
        .await
        .unwrap();
    server.wait_ext_state("alpha", "Ready").await.unwrap();

    let token = server.token_add("p").await.unwrap();
    let url = sub_url(&server);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = Collector {
        seen: seen.clone(),
        delay: Duration::ZERO,
    };
    let client = Client::builder(url, token).pattern("alpha.>").build();
    let run = tokio::spawn(async move { client.run(handler).await });

    // Wait for the subscriber connection to register, then revoke the token.
    server
        .wait_status(|status| {
            status["subscribers"]
                .as_array()
                .is_some_and(|subs| subs.iter().any(|s| s["name"] == "p"))
        })
        .await
        .unwrap();
    server
        .control(ControlRequest::TokenRevoke {
            name: "p".to_string(),
        })
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("run() should return after revoke")
        .expect("run task panicked");
    assert!(
        matches!(result, Err(Error::Revoked)),
        "expected fatal Revoked, got {result:?}"
    );
}

// ---- conformance item 6: lagged → reconnect + resume recovers all events --

#[tokio::test]
async fn lagged_is_recovered_by_run_reconnect_and_replay() {
    const EVENTS: u64 = 8;
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

    let token = server.token_add("p").await.unwrap();
    let url = sub_url(&server);
    let seen = Arc::new(Mutex::new(Vec::new()));
    // A shared cursor store so the (few) reconnects resume from the last
    // processed seq even though run owns its ResumeState internally.
    let store = Arc::new(MemoryCursorStore::new(0));
    let handler = Collector {
        seen: seen.clone(),
        delay: Duration::from_millis(20), // stay slow to force evictions
    };
    let client = Client::builder(url, token)
        .pattern("alpha.>")
        .cursor_store(store.clone())
        .build();
    let run = tokio::spawn(async move { client.run(handler).await });

    // Wait for the connection, then fire a burst the slow handler can't keep up
    // with: the server evicts, emits `lagged`, and the kit must recover.
    server
        .wait_status(|status| {
            status["subscribers"]
                .as_array()
                .is_some_and(|subs| subs.iter().any(|s| s["name"] == "p"))
        })
        .await
        .unwrap();
    for n in 0..EVENTS {
        let body = vec![b'a' + (n % 26) as u8; PAYLOAD_BYTES];
        post(&server, &body).await;
    }

    // Poll until every seq 1..=EVENTS has been handled exactly once.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        {
            let mut got = seen.lock().unwrap().clone();
            got.sort();
            got.dedup();
            if got == (1..=EVENTS).collect::<Vec<_>>() {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "lagged recovery never delivered every event; seen={:?}",
            seen.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Exactly-once: no seq processed twice across reconnects.
    let got = seen.lock().unwrap().clone();
    let mut unique = got.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        got.len(),
        unique.len(),
        "dedup-by-id must prevent double handling across reconnects; seen={got:?}"
    );

    run.abort();
}
