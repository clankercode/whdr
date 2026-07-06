# Subscribers — quickstart

This guide shows how to consume whdr events from a Rust program using the
official client crate, [`whdr-sub-kit`](../crates/whdr-sub-kit). It is the
reference implementation of the *Subscriber wire protocol v2* (see SPEC §9 and
§9.4); client libraries in other languages mirror its behaviour.

The subscriber plane is a token-authenticated WebSocket at
`ws://<host>:<sub_port>/subscribe` (default port `8788`). Events are fanned out
live; with **durable delivery** enabled on the server, a subscriber can also
resume from a cursor and replay events it missed while offline — at-least-once,
de-duplicated by event `id`.

## 1. Mint a subscriber token

Tokens are minted by the operator over the admin control socket with the `whdr`
CLI. The socket path is whatever `[server] control_socket` points at
(`/run/whdr/ctl.sock` by default):

```bash
whdr --socket /run/whdr/ctl.sock token add my-subscriber
# prints:  my-subscriber: tok_XXXXXXXXXXXXXXXXXXXXXXXX
```

Copy the `tok_…` value — it is shown once. Revoke or rotate later with
`whdr token revoke my-subscriber` / `whdr token rotate my-subscriber`, and list
active tokens with `whdr token list`.

## 2. (Optional) enable durable delivery for replay

Live-only delivery needs no server config beyond the subscriber plane. To let
subscribers **resume and replay**, enable the opt-in `[delivery]` section in the
server config (off by default) and SIGHUP or restart the server:

```toml
[delivery]
enabled        = true
store_path     = "/var/lib/whdr/delivery.redb"
retention_secs = 86400   # 24h TTL
```

Without this, a `replay` request is politely refused (`error` op `replay`) and
the subscriber gets live delivery only — the kit keeps working either way.

## 3. A minimal subscriber binary

New Cargo project (`cargo new my-subscriber`), then in `Cargo.toml`:

```toml
[dependencies]
whdr-sub-kit = { path = "../whdr/crates/whdr-sub-kit" } # or a published version
tokio = { version = "1", features = ["full"] }
anyhow = "1"
async-trait = "0.1"
```

`src/main.rs`:

```rust
use whdr_sub_kit::{Client, DeliveredEvent, Handler};

struct Printer;

#[async_trait::async_trait]
impl Handler for Printer {
    async fn on_event(&mut self, event: &DeliveredEvent) -> anyhow::Result<()> {
        let body = event.payload()?; // base64-decoded raw bytes
        println!(
            "seq={} channel={} {} bytes",
            event.seq,
            event.channel,
            body.len()
        );
        Ok(())
    }

    // Explicit, logged data-loss signal (cursor predated retention).
    async fn on_replay_gap(&mut self, from_seq: u64, earliest_seq: u64) -> anyhow::Result<()> {
        eprintln!("replay_gap: events ({from_seq}, {earliest_seq}) were pruned");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let token = std::env::var("WHDR_TOKEN")?;
    let client = Client::builder("ws://127.0.0.1:8788/subscribe", token)
        .pattern("github.>")   // NATS-style; add more with .pattern()/.patterns()
        .resume_cursor(0)      // 0 = replay from the start of retention
        .build();

    // Runs forever, reconnecting with exponential backoff. Returns only on a
    // fatal error: a revoked/absent token or a handler failure.
    client.run(Printer).await?;
    Ok(())
}
```

Run it:

```bash
WHDR_TOKEN=tok_XXXX cargo run
```

Trigger a webhook (e.g. `curl -X POST http://127.0.0.1:8787/dev -d hello`) and
watch the event print.

## What `run` does for you

`Client::run` implements the full reconnect-and-resume algorithm from the
protocol appendix, so you don't have to:

- Authenticates with `Authorization: Bearer <token>`; a `401` is fatal.
- Waits for `welcome`, then subscribes with `replay.after_seq = cursor` on every
  (re)connect.
- De-duplicates by event `id` and skips `seq <= cursor`, so your handler sees
  each event **at most once** even across the replay/live boundary.
- Advances the cursor **only after** your handler returns `Ok` — at-least-once.
- Recovers from a slow-consumer `lagged` eviction and dropped sockets by
  reconnecting and replaying from the cursor.
- Surfaces `replay_gap` (permanent, pruned loss) to `Handler::on_replay_gap`.
- Treats a `closing` `revoked` as fatal (renew your token) and `shutdown` as a
  backoff reconnect.

## Persisting the cursor across restarts

For at-least-once delivery that survives a process restart, persist the cursor.
Implement [`CursorStore`](../crates/whdr-sub-kit) (async `load`/`save`) — e.g.
writing the `u64` to a file — and install it with `.cursor_store(Arc::new(...))`.
`save` is called after each successfully-handled event; `load` supplies the
starting cursor. Without a store, the default in-memory cursor is used (no
cross-restart durability).

## Driving the stream yourself

For bespoke control, use `Client::connect` to get a [`Connection`] and drive
`Connection::recv()` (a typed `SubServerMsg` stream that answers WebSocket pings
and skips unknown frames), applying `ResumeState` for the dedup/cursor guard.
`Client::run` is the recommended path for almost all callers.
