# whdr — Webhook Dynamic Router — Specification

**Version:** 0.1 (MVP spec) · **Status:** draft for review · **Language:** Rust · **Platform:** Unix (Linux/macOS) only for MVP

This spec normatively defines the MVP. RFC-2119 keywords (MUST/SHOULD/MAY) are used in their
usual sense. Items marked **[Dn]** are decisions that resolve gaps or ambiguities in the
original design doc; see the Decision Log (§15) for rationale and the veto surface.

---

## 1. Purpose and scope

whdr is a single-node persistent daemon that:

1. ingests webhooks over HTTP from external providers,
2. routes each request to a long-lived, supervised **extension** process that parses,
   verifies, and translates it into zero or more **events**,
3. fans events out in-memory to **subscriber** projects connected over the subscriber
   WebSocket listener, typically as internal LAN WebSocket connections.

Extensions install like `git`/`cargo` subcommands (`whdr-ext-<id>` on `PATH`). Subscribers
require no install and no server-side configuration.

### 1.1 Goals

- **G1 — Hot source addition.** New provider = install one binary + enable + `SIGHUP`. No host rebuild, no restart.
- **G2 — Zero-config consumers.** New project = connect + subscribe. No install, no server config change.
- **G3 — Small pure core.** Routing is pure functions over immutable snapshots; all IO and mutation at the edges, serialized through one command channel.
- **G4 — Secret hygiene.** Secrets never on argv, never logged, never persisted outside the host store.
- **G5 — Signature integrity.** Verification over exact raw bytes at the ingest edge, inside the ext.

### 1.2 Non-goals (MVP)

- Durable delivery / replay is now implemented but **opt-in and off by default** (§10, `[delivery]`); the default build is still fire-and-forget.
- Outbound webhook sending.
- Untrusted / third-party extensions (no sandbox; roadmap: WASM boundary).
- Horizontal scale / clustering. **Single *server* node** — but subscribers may run on any host
  on the internal network (§9).
- Windows support (SIGHUP + Unix sockets assumed). **[D12]**

---

## 2. Terminology

| Term | Meaning |
|------|---------|
| **whdr-server** | The daemon: HTTP ingest, extension supervisor, routing tables, subscriber socket, control socket. |
| **Extension (ext)** | A `whdr-ext-<id>` binary on `PATH`; a long-lived supervised child that handles one provider or produces events on its own (poller, ws-consumer). |
| **Provider** | The external source an ext handles (GitHub, Teams, Stripe…). One ext per provider. |
| **Subscriber / project** | A consumer connected to the subscriber socket with one or more channel-pattern subscriptions. |
| **Channel** | Dotted event name, e.g. `github.push`. Grammar in §8. |
| **Ext id** | Canonical identifier derived from the binary name (`whdr-ext-github` → `github`), overridable at register (§6.2). |

The two axes remain deliberately separate: **exts are the plug-in mechanism on the source
side; subscribers are dynamic by construction on the consumer side.**

---

## 3. Architecture

```
  GitHub ─┐                            ┌~~WS/LAN~~> project A  (subscribe github.>)   [host 2]
  Teams  ─┼─HTTP──> whdr-server ───────┼~~WS/LAN~~> project B  (subscribe stripe.> …)  [host 3]
  Stripe ─┘           │  ▲             └~~WS/LAN~~> project C  (subscribe >)            [host 1]
                      │  │ stdio (ndjson control plane)          ▲
                spawn │  │ register / dispatch / result / event  │ local UDS
                      ▼  │                                       │
              whdr-ext-github   whdr-ext-teams   whdr-ext-stripe │
              (long-lived, supervised)                    whdr status (admin, local only)
```

Three planes, three surfaces, deliberately distinct:

- **Ingest** — HTTP on `listen_addr`, external-facing (behind a proxy for public providers).
- **Subscribers** — WebSocket on `sub_addr`, internal LAN, token-authed (§9). Multi-host.
- **Admin** — control UDS, local only (`whdr status`, §13).

Data flow for a webhook:

1. `POST /<path>` arrives; server resolves `path → ext id` from the `paths` table snapshot.
2. Server sends `Dispatch` on the ext's stdin (raw body base64'd, headers, path, method, query, and the relevant secret).
3. Ext verifies signature, parses, and replies with one `Result` carrying the HTTP reply **and** the emitted events.
4. Server answers the HTTP request and fans each event out to every subscriber whose pattern matches its channel.

Exts MAY also emit unsolicited `Event` messages at any time after ready (pollers, ws-consumers).

---

## 4. HTTP ingest

- Server listens on `listen_addr` (default `127.0.0.1:8787`). TLS termination is out of scope; front with a reverse proxy or external tunnel for public exposure. Tunnels and proxies are companion infrastructure, not WHDR runtime extensions.
- Route resolution: first path segment is looked up in the `paths` table. **Every ext is
  automatically routable at `/<id>`; `Register.paths` claims *additional* aliases** (e.g. the
  `github` ext may claim `gh`, making both `POST /github` and `POST /gh` valid). **[D2]**
- Remaining path segments and the query string are passed through to the ext verbatim in
  `Dispatch.path` / `Dispatch.query` — providers like Stripe/Teams sometimes encode routing
  hints there. **[D1]**
- All HTTP methods are dispatched (Teams/Graph validation uses `GET`/`POST` variants); the
  method is passed in `Dispatch.method`. **[D1]**

### 4.1 Ingest responses generated by the server itself

| Condition | Response |
|-----------|----------|
| No ext claims the path | `404` |
| Ext enabled but not yet ready (starting) | `503` + `Retry-After: 1` |
| Ext `Failed` (crashloop) | `503` |
| Body exceeds `max_body_bytes` (default 1 MiB) | `413` **[D8]** |
| Ext in-flight window full (default 64 concurrent dispatches) | `429` |
| Dispatch timeout (`dispatch_timeout`, default 10 s) | `504` **[D5]** |

Everything else — including provider-required handshakes and signature-failure responses —
is the ext's `Result.http`, passed through verbatim.

---

## 5. Control plane (server ⇄ ext)

Newline-delimited JSON over the child's stdio. **stdout is protocol-only; anything a human
should read goes to stderr**, which the server pipes into its own log tagged with the ext id.
**[D9]** A non-JSON line on stdout is a protocol violation: log, count it, and after
`max_protocol_errors` (default 3) kill and restart the ext.

### 5.1 Messages

```rust
// ext → server   (child stdout)
#[serde(tag = "type", rename_all = "snake_case")]
enum ExtMsg {
    Register {
        protocol: u32,               // control-plane version; this spec = 1  [D-proto]
        id: Option<String>,          // override canonical id (see §6.2)
        paths: Vec<String>,          // ADDITIONAL path aliases; /<id> is implicit  [D2]
        channels: Vec<String>,       // channel prefixes this ext will emit under (see §5.3)
        meta: Value,                 // free-form (version, description) for status output
    },
    Result {
        req_id: Uuid,
        http: HttpReply,
        events: Vec<Event>,
    },
    Event  { #[serde(flatten)] ev: Event },   // unsolicited push; valid only after ready
    Log    { level: LogLevel, msg: String },  // structured log; level is an enum  [D14]
}

// server → ext   (child stdin)
#[serde(tag = "type", rename_all = "snake_case")]
enum SrvMsg {
    Dispatch {
        req_id: Uuid,
        method: String,                  // "POST", "GET", ...            [D1]
        path: String,                    // full request path as received [D1]
        query: Option<String>,           //                               [D1]
        headers: Map<String, String>,
        body_b64: String,
        secret: Option<String>,
    },
    Shutdown,
}

enum LogLevel { Trace, Debug, Info, Warn, Error }

struct Event     { channel: String, payload_b64: String }
struct HttpReply { status: u16, headers: Map<String, String>, body: String } // headers added [D1]
```

### 5.2 Contract rules

- **Body is base64.** HMAC is computed over exact raw bytes; JSON re-encoding a UTF-8 string
  would corrupt signatures.
- **Secrets travel on stdin, never argv** (argv is visible in `ps`). Exts stay stateless
  about secrets even though they're long-lived.
- **`Result` correlates by `req_id` and MAY arrive out of order** relative to other in-flight
  dispatches. Exts MAY process dispatches concurrently or serially. **[D10]**
- **Duplicate `Result` for a `req_id`**, or a `Result` for an unknown/expired `req_id`, is
  logged and dropped (the HTTP reply already went out on timeout).
- `HttpReply.headers` lets exts satisfy handshakes that require a specific `Content-Type`
  (e.g. Teams/Graph `validationToken` echo as `text/plain`). **[D1]**
- **Protocol versioning:** server rejects `Register.protocol` values it doesn't support and
  marks the ext `Failed` with a clear status message.

### 5.3 Channel namespace enforcement **[D-ns]**

An ext MAY only emit events whose channel's **first segment** is its canonical id or one of
the prefixes declared in `Register.channels` (subject to the same collision rules as ids,
§6.2). An event outside the ext's namespace is dropped and logged as a protocol error. This
prevents one ext from spoofing another's channels (`whdr-ext-foo` emitting `github.push`).

### 5.4 Backpressure and pipe discipline

- The server runs one dedicated writer task and one dedicated reader task per child, each with
  bounded internal channels — never a synchronous write-then-read on the same task, which can
  deadlock when both pipe buffers fill.
- Per-ext in-flight dispatch window: `max_in_flight` (default 64). Beyond it, ingest returns
  `429` rather than queueing unboundedly.

---

## 6. Extension lifecycle

### 6.1 State machine

```
             scan ∩ enabled                register ok
  Discovered ───────────────> Starting ────────────────> Ready
                                 │  register timeout /        │ crash
                                 │  collision / bad proto     ▼
                                 └──────────> Failed <── Backoff ──respawn──> Starting
                                                 ▲   (crashloop threshold)
```

1. **Scan** — on boot and on `SIGHUP`, scan `PATH` for `whdr-ext-*`; the suffix is a candidate id.
2. **Enable gate** — intersect candidates with the **enabled** set from config. *Discovery ≠
   autostart.* (`autostart_all = true` available for loose/dev mode.)
3. **Start** — spawn; the ext MUST send `Register` within `register_timeout` (default 5 s).
   No register → kill, mark `Failed`.
4. **Ready** — insert path/channel claims, begin routing. Requests arriving while `Starting`
   get `503 Retry-After` (§4.1); unsolicited `Event`s sent before ready are dropped and
   counted as protocol errors.
5. **Supervise** — child exit → exponential backoff respawn (base 500 ms, factor 2, cap 30 s);
   more than `crashloop_threshold` (default 5) exits within `crashloop_window` (default 60 s)
   → mark `Failed`, surface in status, stop retrying until the next `SIGHUP`.
   **Hang detection:** `hang_kill_threshold` (default 3) *consecutive* dispatch timeouts with
   zero intervening successful `Result`s → treat as hung: kill and restart through the same
   backoff path. **[D7]**
6. **Rescan (`SIGHUP`)** — diff `PATH ∩ enabled` against the running set; start the new,
   drain the removed. Hot install: `cargo install whdr-ext-stripe` → add to enabled →
   `kill -HUP $(pidof whdr-server)`.

### 6.2 Identity and collisions

- Canonical id derives from the binary name (`whdr-ext-github` → `github`): deterministic,
  dedupes for free.
- `Register.id` MAY override the canonical id; `Register.paths` / `Register.channels` claim
  additional aliases and namespaces.
- **Collision is an error.** If a claimed id, path, or channel prefix is already held by a
  Ready/Starting ext, reject the register, kill the newcomer, mark it `Failed`. First claim
  wins; two binaries never silently fight over `github`.

### 6.3 Drain and shutdown **[D11]**

To drain an ext (rescan removal, or server shutdown):

1. Remove its claims from the routing snapshot (new requests → 404/503 immediately).
2. Wait up to `drain_timeout` (default 5 s) for in-flight `Result`s.
3. Send `Shutdown` on stdin; close stdin.
4. After `term_grace` (default 3 s): `SIGTERM`; after another `term_grace`: `SIGKILL`.

Server shutdown (`SIGTERM`/`SIGINT`) drains all exts concurrently, closes subscriber
connections with a final `{"type":"closing"}` frame, then exits.

---

## 7. Routing data model

Four tables — the original doc's three, plus the connection registry the fan-out actually
needs **[D13]**:

| Table | Shape | Purpose |
|-------|-------|---------|
| `paths` | `HashMap<String, ExtId>` | inbound HTTP path → ext |
| `procs` | `HashMap<ExtId, ProcHandle>` | process registry + supervision state |
| `subs`  | `HashMap<SubId, SmallVec<Pattern>>` | subscriber → its patterns |
| `conns` | `HashMap<SubId, mpsc::Sender<Event>>` | subscriber → bounded outbound queue |

Note `subs` is keyed by subscriber, not by pattern **[D13]**: fan-out is "for each subscriber,
does any of its patterns match?" — O(subscribers × patterns) per event, which is fine at MVP
scale (dozens of subscribers) and avoids pattern-key canonicalization headaches. Revisit with
a trie if subscriber counts grow.

The **supervisor is the only stateful island** — it owns `procs` and restart policy. Every
table mutation is serialized through a single mpsc command channel; readers work off immutable
snapshots (`arc-swap`). "Route this request" and "match this channel" are pure functions over
a snapshot.

---

## 8. Channel grammar **[D3]**

NATS-style subjects, adopted verbatim because the grammar is proven, unambiguous, and
already familiar:

- A **channel** is 1+ tokens joined by `.`; token = `[a-z0-9_-]+`. Channels contain no wildcards.
- A **pattern** is tokens joined by `.` where a token may be `*` (matches exactly one token)
  and the final token may be `>` (matches one or more trailing tokens).
- Examples: `github.push` (exact) · `github.*` (matches `github.push`, not
  `github.pr.opened`) · `github.>` (all github events) · `>` (everything).

> Consequence for the design doc's examples: "all stripe events" is `stripe.>`, not
> `stripe.*`. The `*`-as-suffix-glob reading was ambiguous; this resolves it.

Invalid channels from exts are dropped + logged (§5.3); invalid patterns from subscribers get
an `error` reply (§9).

---

## 9. Subscriber interface

**Transport: WebSocket over TCP, on a dedicated internal listener (`sub_addr`), separate from
the HTTP ingest listener.** **[D4]** Subscribers may run on any host that can reach `sub_addr`;
the plane is intended for a **trusted internal network only** — never expose `sub_addr` to the
public internet. The JSON message types below are the same across any future transport (raw
TCP, etc.); WebSocket just supplies framing (one message per text frame) and a standard
handshake to carry auth.

Why a *separate* listener and not the ingest one: `listen_addr` receives provider webhooks
that originate externally (GitHub, Stripe, Teams), so in practice it sits behind an
internet-facing proxy. The subscriber plane is internal-only and must not share that surface.
**[D-sep]**

### 9.1 Connection and auth **[D-auth]**

- Client opens a WebSocket to `ws://<host>:<sub_port>/subscribe`. For TLS in this
  release, terminate `wss://` at a reverse proxy and forward to the subscriber listener (§9.3).
- The handshake MUST carry `Authorization: Bearer <token>`. The server hashes the presented
  token and looks the hash up in the **token store** (§11.1). Unknown or missing token → the
  upgrade is rejected with `401` and the connection closed. Comparison is constant-time.
- The matched name becomes the subscriber's identity: it labels the connection in
  `whdr status` and scopes its `delivered`/`dropped` counters. Two connections may present the
  same token (same name); they're distinct connections sharing a label.
- **Tokens are managed at runtime via the CLI** — mint, rotate, revoke, list — without a
  restart (§11.1, §13.1). Changes take effect immediately *and* persist.
- **Revocation is effective:** revoking (or rotating) a token immediately closes any live
  connection using it, with `{"type":"closing","reason":"revoked"}`. `SIGHUP` also reloads the
  store, so an out-of-band restore/replace is picked up too.

### 9.2 Protocol (post-handshake)

Same JSON messages as before, one per WebSocket text frame:

```jsonc
// client → server
{"type":"subscribe",   "patterns":["github.>"], "replay":{"after_seq":128}}  // replay optional; omit for live-only
{"type":"unsubscribe", "patterns":["github.>"]}
{"type":"ping"}

// server → client
{"type":"welcome", "name":"project-a"}                 // first frame after auth; echoes identity
{"type":"ok",      "op":"subscribe"}
{"type":"error",   "op":"subscribe", "msg":"invalid pattern: 'github.>x'"}
{"type":"event",   "id":"7d9c…-uuid", "seq":128, "ts_ms":1751760000000, "channel":"github.push", "payload_b64":"..."}
{"type":"replayed",  "through_seq":128}                 // replay window fully delivered
{"type":"replay_gap","from_seq":10, "earliest_seq":57}  // requested cursor predates retention
{"type":"lagged",    "dropped":42}                      // outbound queue evicted events; reconnect+replay
{"type":"pong"}
{"type":"closing", "reason":"shutdown"}                // reason ∈ shutdown | revoked
```

- **Event identity [D-evid]:** the server stamps every event frame with a UUID `id` and a
  unix-milliseconds `ts_ms` at fan-out. All subscribers see the same `id` for the same event —
  the replay/dedup key. Extensions do not supply
  these fields; the ext-facing `Event` message is unchanged.
  A global monotonic `seq` is also stamped; it is the replay cursor (§9.4) and is additive (old clients ignore it).
- Subscriptions are per-connection and die with it. Zero install, zero server-side config
  *beyond issuing a token* with the daemon-managed CLI flow
  (`whdr token add <name>`).
- WebSocket ping/pong (control frames) are used for liveness in addition to the app-level
  `ping`/`pong`; a subscriber that fails to answer WS pings within `ws_idle_timeout`
  (default 30 s) is dropped.
- **Slow-consumer policy [D-slow]:** each connection has a bounded outbound queue capped
  both by frame count (`sub_queue_len`, default 1024) and by bytes (`sub_queue_bytes`,
  default 8 MiB) — the byte cap bounds worst-case memory per connection regardless of
  payload size. On overflow the **oldest queued event is evicted** (webhook consumers care
  about freshness more than completeness) and the subscriber's `dropped` counter increments
  (visible in `whdr status`). Event frames are serialized once and shared across all
  subscriber queues. Control frames (ok/error/pong/closing) are never evicted. The
  connection is *not* killed — consistent with MVP fire-and-forget semantics, and
  drop-counts make the loss observable instead of silent.

### 9.3 TLS **[D-tls]**

Tokens cross the network, so on an untrusted segment they need TLS. Native subscriber TLS is
**not implemented/publish-ready in this release**: configuring `[subscribers.tls]` is rejected
with `subscriber TLS is configured but native TLS is not implemented`.

Supported deployment options today:

1. **Proxy-terminated TLS** — front `sub_addr` with a reverse proxy that owns TLS, same as ingest.
2. **Explicit LAN plaintext** — set `allow_plaintext_lan = true` only for a reviewed trusted LAN.

**Guardrail:** if `sub_addr` binds to anything other than a loopback address and
`allow_plaintext_lan = true` is not set, the server **refuses to start**. This forces
plaintext-over-LAN to be a deliberate, reviewable choice rather than an accident.

### 9.4 Resume and replay **[D-replay]**

Replay is **opt-in per connection**: a client resumes by sending `replay:{"after_seq":<cursor>}`
on `subscribe`. When `[delivery] enabled`, the server streams the stored events with
`seq > after_seq` matching that connection's patterns, then a `replayed` frame carrying the head
`seq` it caught up to, then live delivery continues. A cursor that predates the retained floor
yields an explicit `replay_gap` (with the earliest still-available `seq`) before replay resumes
from that floor — pruned loss is never silent. When durability is off, a `replay` request is
refused with `error` op `replay` while the live subscription still succeeds. Delivery is
**at-least-once**: the server holds no per-subscriber offset (the cursor is client-owned), so a
frame may arrive twice around the replay/live boundary; the client **dedups by `id`** and advances
its cursor by the highest `seq` seen. A slow-consumer drop (§9, [D-slow]) surfaces a `lagged`
frame; the client recovers by reconnecting and replaying from its cursor. The full client-facing
contract is the **Subscriber wire protocol v2** appendix in the durable-delivery plan.

---

## 10. Delivery semantics

**MVP — fire-and-forget.** In-memory fan-out only. Offline subscriber → event lost. Slow
subscriber → drops per §9. Accepted tradeoff for shipping; loss is *counted*, never silent.

**Durable delivery — at-least-once, opt-in ([D-store], [D-replay]).** When `[delivery] enabled`
(default `false`), every fanned-out event is appended to an embedded **redb** log keyed by a
global monotonic `seq`, before delivery. Subscribers resume by sending `replay.after_seq` on
`subscribe` (§9.4): the server streams stored events `> after_seq` matching their patterns, then
`replayed`. Retention is TTL-bounded (`retention_secs`, default 24 h) plus a size cap
(`max_bytes`, `max_events`); the oldest events are pruned from the front. A cursor that predates
the retained floor yields an explicit `replay_gap`, never silent loss. Delivery is at-least-once:
clients dedup by the stable event `id` and advance their cursor by `seq`. The slow-consumer drop
(§9, [D-slow]) is recoverable — a dropped connection receives a `lagged` frame and reconnects with
its cursor to replay the gap. **Never persisted:** subscriber tokens, provider secrets, or
per-subscriber identity — only `{seq, id, ts_ms, channel, payload_b64}`. At rest the store is a
`0600` file in the state dir (§11.2); payloads are not encrypted, so the short TTL bounds exposure
([D-dursec]). When disabled, the path is byte-for-byte the fire-and-forget MVP plus the additive
`seq` field.

---

## 11. Configuration

TOML, default `/etc/whdr/config.toml`, override with `--config`. Secrets live in a
**separate** file so the main config can be committed/reviewed.

```toml
[server]
listen_addr    = "127.0.0.1:8787"   # HTTP ingest (external via proxy)
sub_addr       = "127.0.0.1:8788"   # WebSocket subscriber plane (loopback by default)
control_socket = "/run/whdr/ctl.sock"  # local admin, UDS only
# metrics_addr = "127.0.0.1:9598"   # optional Prometheus text endpoint; loopback only

[subscribers]
token_store        = "/var/lib/whdr/tokens.toml"  # server-managed state (§11.1); NOT hand-edited
allow_plaintext_lan = false          # must be true to bind sub_addr to non-loopback w/o TLS
ws_idle_timeout_ms  = 30_000
# [subscribers.tls] is rejected in this release; use proxy TLS or explicit LAN plaintext.

[extensions]
enabled       = ["github", "teams"]
autostart_all = false

[limits]
max_body_bytes      = 1_048_576
max_in_flight       = 64
sub_queue_len       = 1024
sub_queue_bytes     = 8_388_608     # per-connection outbound byte budget
dispatch_timeout_ms = 10_000

[timeouts]
register_ms    = 5_000
drain_ms       = 5_000
term_grace_ms  = 3_000

[delivery]                                   # opt-in durable delivery / replay (§11.2); off by default
enabled              = false                 # off = no persistence, replay refused
store_path           = "/var/lib/whdr/delivery.redb"
retention_secs       = 86_400                # 24h TTL
max_bytes            = 536_870_912           # 512 MiB size cap (sum of stored value bytes)
max_events           = 1_000_000             # hard cap on retained event count
prune_interval_secs  = 300                   # background prune cadence

[secrets]
file = "/etc/whdr/secrets.toml"   # must be mode 0600, owned by the whdr user; refuse to start otherwise
```

```toml
# /etc/whdr/secrets.toml — keyed by ext id  [D6]
github = "whsec_..."
teams  = "..."
```

**[D6]** MVP: **one secret per ext id.** Multiple endpoints with distinct secrets under one
provider (e.g. two GitHub webhooks) is a real case but roadmap: the schema extends naturally
to per-path overrides (`[github] default = "...", "gh-org2" = "..."`) without a breaking
change, so deferring costs nothing now.

`SIGHUP` reloads config (enabled set, secrets, token store, limits, delivery) and triggers the rescan.

### 11.1 Subscriber token store **[D-tokmgmt]**

Subscriber tokens are **server-managed state, not hand-edited config** — that's the key
difference from `secrets.toml` (which stays operator-owned plaintext, because HMAC needs the
raw provider secret). The store:

- Lives in the **state** dir (`/var/lib/whdr/`), mode `0600`, owned by the whdr user. It is
  *the daemon's* file; operators mutate it only through the CLI, never a text editor.
- Stores a **hash of each token, never the token itself.** Tokens are high-entropy random
  (`tok_` + 32 CSPRNG bytes, base64url), so a fast hash (SHA-256) suffices — no KDF needed,
  and the file at rest holds no usable credential.
- Format:

```toml
# /var/lib/whdr/tokens.toml — SERVER-MANAGED. Do not hand-edit.
[project-a]
hash    = "sha256:1a2b…"          # of the bearer token
created = "2026-07-05T04:00:00Z"

[project-b]
hash    = "sha256:9f8e…"
created = "2026-07-04T21:12:00Z"
```

**Write discipline.** The server is the single writer. Mutations funnel through the same mpsc
command channel as every other table (§7), and each persist is an **atomic replace**: write
`tokens.toml.tmp` in the same dir, `fsync`, `rename` over the target (preserving `0600`). A
crash mid-write leaves the previous store intact — never a truncated file.

**Consistency.** A CLI mutation updates the in-memory hash set *and* the on-disk store in the
same command, so runtime state and persisted state never diverge. Restart reloads the store;
tokens survive. Because the token value is only ever known at mint time, tokens are
**show-once** — lose one, rotate it (§13.1).

### 11.2 Delivery log **[D-store]**

The durable delivery log is a single embedded **redb** file in the state dir
(`/var/lib/whdr/delivery.redb` by default), created `0600` in a `0700` dir and owned by the whdr
user. It is opt-in (`[delivery] enabled`, default `false`); when off, no file is created and the
fan-out hot path takes no fsync.

- **Single-writer, crash-safe.** A serialized writer allocates a contiguous `seq` run, writes all
  rows of a fan-out batch in **one** redb transaction, and commits (one fsync) *before* any frame
  is delivered — so an event is durable before it is sent. redb's ACID commit gives torn-write
  safety, mirroring the token-store write discipline in §11.1.
- **Gapless seq.** The log is gapless by construction; the only gaps are the pruned front, which
  raises the retained floor. On boot the file is scanned once to recover head/floor/counters.
- **Retention.** Pruned from the front while past the TTL (`retention_secs`) **or** over the size
  caps (`max_bytes`, `max_events`), on a background cadence (`prune_interval_secs`). Front = lowest
  seq = oldest, because seq order == arrival order under the single writer.
- **At rest.** On open of an existing file the mode is enforced `0600` and the server **refuses to
  start** otherwise (mirrors `secrets.toml`/`tokens.toml`). Stored rows are only
  `{seq, id, ts_ms, channel, payload_b64}` — never tokens, provider secrets, or per-subscriber
  identity. Payloads are not encrypted; the short TTL bounds exposure ([D-dursec]).

---

## 12. Security

- Secrets: host-owned store, `0600`-enforced file, passed per-`Dispatch` on stdin, keyed by
  ext id. Never argv, never logged, never persisted to any future queue.
- Signature verification happens **in the ext, at the ingest edge**, over raw bytes.
- Channel namespace enforcement (§5.3) prevents cross-ext event spoofing.
- Enable list is explicit and reviewable; discovery never auto-runs an unlisted binary unless
  `autostart_all` is deliberately set.
- **Subscriber plane (`sub_addr`) is network-exposed and therefore token-gated.** Tokens are
  minted by the daemon (CSPRNG), stored **hashed** in a `0600` server-managed file keyed by
  subscriber name (§11.1) — the file at rest holds no usable credential. The handshake requires
  `Authorization: Bearer`; the presented token is hashed and compared constant-time; unknown
  tokens are rejected at upgrade; revoke/rotate is immediate (§9.1, §13.1). Intended for a
  trusted internal LAN — **never the public internet.**
- **The control socket is the mint capability.** It's local UDS with filesystem permissions;
  whoever can reach it can add/rotate/revoke tokens. Keep its perms tight; it is never exposed
  over the network.
- **TLS for the subscriber plane** is required on untrusted segments, but native subscriber TLS
  is not implemented in this release. Use proxy TLS; `[subscribers.tls]` is rejected. The server
  refuses to bind `sub_addr` to a non-loopback address unless `allow_plaintext_lan = true` is set
  explicitly (§9.3).
- **Control/admin socket stays local UDS** with filesystem permissions; `whdr status` is never
  exposed over the network.
- Exts are trusted native processes (no sandbox). Untrusted/third-party exts would need a
  WASM boundary (e.g. Extism) — out of scope.
- Ingest listens on loopback by default; public exposure goes through a reverse proxy that
  owns TLS and can add rate limiting.

---

## 13. Observability & admin

The **control socket** (`control_socket`, UDS, mode `0660`, local only) is the admin plane:
request/response ndjson. Reachability to this socket *is* the admin capability — anyone who can
open it can mint tokens — so it stays a local UDS gated by filesystem permissions, never
network-exposed.

- `{"type":"status"}` returns uptime, per-ext `{id, state, pid, restarts, paths, channels,
  in_flight, protocol_errors, consecutive_timeouts, events_emitted, last_event_at_ms}`,
  per-subscriber `{name, remote_addr, patterns, delivered, dropped}`, global counters, and a
  `delivery` object `{enabled, head_seq, floor_seq, retained_events, retained_bytes,
  persist_errors}` (just `{enabled:false}` when durability is off).
  `delivered` counts events handed to the connection writer (post-eviction);
  `dropped` counts events evicted or rejected by the queue budgets — together they account
  for every event that matched the subscriber's patterns.
  `last_event_at_ms` exists because pure pollers cannot hang-detect via dispatch timeouts
  [D7]; their failure mode is silence, and recency makes that silence visible.
- **`whdr status`** renders that JSON as a table; `whdr status --json` passes it through.
- Server logs via `tracing`; ext stderr lines are ingested and tagged `ext=<id>`; `ExtMsg::Log`
  maps levels onto the same subscriber.
- **Prometheus metrics [D-metrics]:** setting `metrics_addr` serves `GET /metrics`
  (text format 0.0.4) rendered from the same status document the control socket returns, so
  the two admin surfaces cannot disagree. The listener refuses non-loopback binds — metrics
  stay on the admin plane; scrape locally or relay via a proxy. Disabled by default. When
  durable delivery is enabled, `whdr_delivery_{enabled,head_seq,floor_seq,retained_events,
  retained_bytes,persist_errors}` gauges are exported.

### 13.1 Token management (runtime, persistent) **[D-tokmgmt]**

Token lifecycle is driven entirely from the CLI over the control socket — no restart, no file
editing — and every change is persisted atomically to the store (§11.1) before the command
returns.

| CLI | Control message | Server response | Effect |
|-----|-----------------|-----------------|--------|
| `whdr token add <name>` | `{"type":"token.add","name":"…"}` | `{"type":"token","name":"…","token":"tok_…"}` | Mints a token, stores its hash, prints the value **once**. Errors if the name exists. |
| `whdr token rotate <name>` | `{"type":"token.rotate","name":"…"}` | `{"type":"token","name":"…","token":"tok_…"}` | Mints a new value for an existing name, invalidates the old, closes live connections on it. |
| `whdr token revoke <name>` | `{"type":"token.revoke","name":"…"}` | `{"type":"ok"}` | Removes the name, closes its live connections (`reason:"revoked"`). |
| `whdr token list` | `{"type":"token.list"}` | `{"type":"tokens","tokens":[{name,fingerprint,created,active_conns}]}` | Lists names + a short non-reversible fingerprint (e.g. last 4 of the hash) + created time + live connection count. Never reprints token values. |

Typical flow: `whdr token add project-c` → copy the `tok_…` into project-c's config → it
connects. Lost a token? `whdr token rotate project-c`. Decommissioning? `whdr token revoke`.
All four survive a restart because they mutate the persisted store, not just memory.

---

## 14. Limits & defaults (single reference table)

| Knob | Default | On breach |
|------|---------|-----------|
| `max_body_bytes` | 1 MiB | 413 |
| `max_in_flight` (per ext) | 64 | 429 |
| `dispatch_timeout` | 10 s | 504; counts toward hang detection |
| `hang_kill_threshold` | 3 consecutive timeouts | kill + backoff restart |
| `register_timeout` | 5 s | kill, `Failed` |
| `max_protocol_errors` | 3 | kill + backoff restart |
| crashloop | 5 exits / 60 s | `Failed` until `SIGHUP` |
| backoff | 500 ms × 2ⁿ, cap 30 s | — |
| `sub_queue_len` | 1024 events | evict oldest + count |
| `sub_queue_bytes` | 8 MiB per connection | evict oldest + count |
| `ws_idle_timeout` | 30 s | drop subscriber (missed WS pings) |
| `drain_timeout` / `term_grace` | 5 s / 3 s | escalate TERM → KILL |
| `delivery.enabled` | `false` | replay requests refused with `error` op `replay` |
| `delivery.retention_secs` | 24 h | oldest events pruned; requested-but-pruned seq → `replay_gap` |
| `delivery.max_bytes` | 512 MiB | oldest events pruned to fit |
| `delivery.max_events` | 1,000,000 | oldest events pruned to fit |
| `delivery.prune_interval_secs` | 300 s | — |

---

## 15. Decision log (veto surface)

Each open question or gap in the design doc, closed with a decisive call. Any of these is
cheap to reverse *now* and expensive later — flag vetoes before M2 of the plan.

| # | Decision | Rejected alternative & why |
|---|----------|---------------------------|
| D1 | `Dispatch` carries `method`/`path`/`query`; `HttpReply` carries `headers` | Without path, a multi-path ext can't tell endpoints apart; without reply headers, the Teams `text/plain` echo handshake is impossible. Not optional. |
| D2 | `/<id>` always routable; `Register.paths` = extra aliases | Original doc contradicted itself (`POST /gh → github` vs. "the path segment *is* the id"). This keeps the zero-config default *and* explains the `/gh` example. |
| D3 | NATS grammar: `*` = one token, `>` = tail | Suffix-glob `*` is ambiguous (does `stripe.*` match `stripe.charge.succeeded`?). NATS semantics are specified, proven, and familiar. |
| D4 | Subscriber transport = **WebSocket** on a dedicated internal listener, per-subscriber bearer tokens | Constraint changed post-v0.1: subscribers now run on other hosts. See revised IGC below. |
| D5 | 10 s dispatch timeout → 504 | Unbounded wait lets one hung ext pin server resources and provider retry queues. |
| D6 | One secret per ext id (per-path = roadmap) | Schema extends compatibly later; multi-secret-per-provider isn't needed by the launch exts (github, teams). |
| D7 | Hang detection = consecutive-timeout kill, no ping/pong | A ping frame adds protocol surface; timeouts already measure the thing that matters (can it serve dispatches?). Pure pollers that take no dispatches can't hang-detect this way — acceptable: their failure mode is silence, visible in status via event counters. |
| D10 | Out-of-order `Result` explicitly legal | Forcing serial replies would serialize slow providers behind fast ones inside one ext. |
| D11 | Drain: unroute → wait 5 s → `Shutdown` → TERM → KILL | Undefined drain semantics = dropped in-flight webhooks on every rescan. |
| D13 | `subs` keyed by SubId; add `conns` table | Pattern-keyed map can't be delivered to without a SubId→connection map anyway, and pattern keys need canonicalization. Linear match is fine at MVP scale. |
| D-ns | Exts may only emit under their claimed channel prefixes | Without it any ext can spoof any other's events; one-line check, real containment win. |
| D-slow | Slow subscriber: evict **oldest** + count, keep connection; queues capped by count *and* bytes | Disconnecting punishes a consumer for a burst; silent drop hides the problem. Drop-newest (v0.1) kept stale events while losing current ones — for webhooks, freshness wins. A count-only cap left worst-case memory unbounded in payload size; the byte budget closes it. |
| D-sep | Subscriber plane is a *separate* listener from HTTP ingest | Ingest is external-facing (provider webhooks originate on the internet); co-mingling would expose the internal consumer plane on that surface. |
| D-auth | Per-subscriber named bearer tokens in a 0600 file; revocation effective on SIGHUP | A single shared token can't be revoked per consumer and gives anonymous connections in status. Named tokens cost one extra map lookup. |
| D-tls | Proxy TLS now; native `[subscribers.tls]` is rejected until implemented; refuse non-loopback plaintext bind unless `allow_plaintext_lan` | Tokens cross the wire; silent plaintext is the accident to prevent. Explicit opt-in matches the enable-list philosophy. |
| D-evid | Server stamps `id` (UUID) + `ts_ms` on every subscriber event frame | Durable delivery/replay/dedup (§10) needs event identity, and the subscriber wire format is expensive to change once consumers exist. Stamping in the server keeps extensions unchanged; additive fields cost nothing now. |
| D-metrics | Optional `/metrics` listener, loopback-only, rendered from the status document | Operators want Prometheus without parsing the UDS status JSON. Loopback-only keeps it on the admin plane; a shared render source means status and metrics cannot diverge. |
| D-tokmgmt | Tokens are daemon-minted, stored **hashed** in a server-owned state file, managed at runtime via CLI over the control socket; atomic write; show-once | (a) CLI writing the file directly = two writers racing + humans needing 0600 write perms — rejected. (b) Plaintext-at-rest storage — rejected: a network auth credential shouldn't sit reusable on disk; hashing is free for high-entropy tokens. (c) Restart-to-apply — rejected: the whole request is runtime mint. Cost: tokens are no longer hand-editable (they're CLI-only) and are show-once — both acceptable, both standard for API keys. |
| D-store | Embedded **redb** single-file log for durable delivery (§11.2) | **Redis** — rejected: a separate networked daemon contradicts whdr's single-node, zero-external-service ethos; it would be the only runtime service dependency. **Hand-rolled segment/append log** — rejected: durability is the whole point, and a bespoke log reintroduces the crash-safety burden a store removes (torn-tail recovery, fsync ordering, segment-roll atomicity, index rebuild). redb gives ACID commits, crash safety, and ordered range scans as one pure-Rust crate. See store-choice IGC below. |
| D-seq | A **global monotonic `u64` seq** stamped per event at fan-out, additive on every event frame | Not exposing seq on live frames — rejected: clients need one ordering/cursor key that is identical live or replayed, and seq is near-free and additive (old clients ignore it). Reusing `ts_ms` as the cursor — rejected: wall-clock is non-unique and non-monotonic across NTP steps. |
| D-replay | **Client-supplied resume cursor** on `subscribe` (`replay.after_seq`); server holds no per-subscriber offset | **Server-tracked per-subscriber offsets + acks** — rejected: §9.1 allows multiple concurrent connections under one token, so a single server-side offset per name is ambiguous and corruptible, needs durable per-subscriber state + an ack protocol, and fights the per-connection subscription model (§9.2). Client-owned cursors match that model, need no acks, and give at-least-once as long as the client persists its cursor and replays on reconnect. |
| D-ret | TTL-bounded (**24 h default**) **plus** a size cap (bytes + event count); prune from the front (lowest seq = oldest) | TTL only — rejected: a burst can blow disk before the TTL fires. Size only — rejected: stale payloads shouldn't linger past the short-exposure window that justifies plaintext-at-rest (D-dursec). Both bounds, front-truncating, because seq order == arrival order == ts order under the single writer. |
| D-gap | A requested `after_seq` below the retained floor yields an explicit **`replay_gap`** (with the earliest available seq), then replay continues from that floor | Silent truncation — rejected: the D-slow philosophy is "loss is counted, never silent"; a replay that quietly skips pruned events is silent loss. The client must learn it missed a range so it can reconcile out-of-band. |
| D-dursec | **Documented file-permission posture** (`0700` dir, `0600` file, whdr-user owned, refuse-to-start on wrong perms), **not** encryption at rest, bounded by the short TTL | Encryption at rest — rejected *for this release*: it needs key management, and a key on the same disk adds negligible protection against the actual single-node threat (filesystem access), while an external KMS reintroduces the external dependency rejected in D-store. Matches the existing `secrets.toml`/`tokens.toml` posture. Payload encryption reserved as a future option (§16). |
| D-dur-optin | Feature is **off by default** (`[delivery] enabled = false`); when off no file is created, no fsync on the hot path, replay is refused | On by default — rejected: durability silently writes possibly-sensitive payloads to disk and adds fsync latency to fan-out; MVP is explicitly fire-and-forget (§10), so enabling must be a deliberate, reviewable operator choice, consistent with the enable-list philosophy. |
| D-lag | The slow-consumer drop (D-slow) gains an explicit **`lagged`** control frame; recovery is via the client's own cursor | Relying on the client to infer loss from seq gaps — rejected: seq is *global*, so gaps are normal (they mark other subscribers' events). A client cannot distinguish "gap because not my channel" from "gap because dropped". Loss must be explicit; the `lagged` frame carries the drop count and the client resumes from its last-received seq. |
| D-dedup | At-least-once with **client dedup by `id`**; server replay is best-effort ascending, not strictly ordered around the live/replay boundary | Server-enforced strict global ordering to every queue (a single fan-out lock spanning every fsync) — rejected: it serialises all delivery behind disk latency for a guarantee the client can cheaply provide. The durable *log* is written by a single serialized writer (gapless, ordered), but frame *delivery* stays concurrent; the client dedups by the stable `id` and advances its cursor by the highest seq seen, tolerating the small replay/live overlap at reconnect without a global lock. |

**IGC — store choice (D-store).** Goals for the backing store: **S-a** crash-safe (a mid-write
crash never corrupts or loses committed events) · **S-b** ordered replay-by-cursor (range scan
from a seq) · **S-c** cheap TTL+size pruning · **S-d** dependency-light (no external service,
minimal/pure-Rust deps, testable in a temp dir) · **S-e** single-node fit.

| Option | All | S-a | S-b | S-c | S-d | S-e |
|--------|-----|-----|-----|-----|-----|-----|
| Redis | ✘ | ✔ | ✔ | ✔ | ✘ (external daemon) | ✘ (built for networked/multi) |
| Hand-rolled segment log | ? | ? (bespoke torn-tail/roll recovery) | ✔ | ✔ (unlink segment) | ✔ | ✔ |
| **redb** | ✔ | ✔ (ACID commit) | ✔ (range scan) | ✔ (front range-delete) | ✔ (one pure-Rust crate) | ✔ |

Redis fails S-d/S-e outright. The segment log clears the hard goals but turns amber on S-a — the
one property the feature exists to guarantee — because crash correctness is hand-rolled. redb
clears every goal with a single pure-Rust dependency. **redb wins decisively;** the segment log
was the only real contender and lost on the property that matters most.

**IGC — subscriber transport (D4), re-run.** The v0.1 spec parked "remote/off-box subscribers"
as an *inactive* goal because the system was single-node with on-box consumers. That
assumption is now false: subscribers run on other LAN hosts. The previously-inactive goal is
the new primary. Re-stated goals: **G-a** zero-install consumers · **G-b** works over LAN /
multi-host (now the driving requirement) · **G-c** auth suitable for an internal network ·
**G-d** mature transport + TLS story (don't hand-roll framing/crypto; proxy TLS until native
subscriber TLS lands) · **G-e** consumer plane isolated from the internet-facing ingest surface.

| Idea | All | G-a | G-b | G-c | G-d | G-e |
|------|-----|-----|-----|-----|-----|-----|
| UDS + ndjson (old pick) | ✘ | ✔ | ✘ | ✔ | ✔ | ✔ |
| Raw TCP + ndjson + hello-token | ? | ✔ | ✔ | ? (bespoke handshake) | ✘ (hand-rolled framing + TLS) | ✔ |
| **WebSocket, own internal listener** | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ |

UDS is eliminated — it fails the now-active G-b outright. Raw TCP clears the two hard goals but
turns amber on auth (I'd define the handshake myself) and fails G-d (I hand-roll framing and
the TLS integration). WebSocket clears the active transport goals: `Authorization: Bearer` rides
the standard handshake (G-c), tokio-tungstenite supplies mature framing, proxy TLS supplies the
release TLS story until native rustls is implemented (G-d), and a dedicated `sub_addr` keeps it
off the ingest socket (G-e). The message *types* are transport-independent, so nothing above the
wire changed — only the framing and handshake did.

**WebSocket wins decisively.** Raw TCP was the only real contender and lost on G-d alone;
if a future consumer can't speak WS, the same JSON messages drop onto a raw-TCP listener with
no protocol change.

---

## 16. Open items deliberately left open

- Payload encryption at rest for the delivery log — reserved; the current posture is 0600 file perms + short TTL ([D-dursec]).
- Server-side compaction beyond TTL+size pruning — deferred.
- WASM sandbox for third-party exts — out of scope until there *are* third-party exts.
- Per-path secrets — schema reserved (§11), implementation deferred.
