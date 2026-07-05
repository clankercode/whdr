# whdr — Implementation Plan

Companion to `SPEC.md` v0.1. Sizes are relative (S ≈ a focused session, M ≈ a few, L ≈ a week
of part-time work), not dates.

---

## 1. Constraint analysis (what actually gates shipping)

Theory-of-Constraints pass over the system: most of whdr is well-trodden Rust (axum/hyper
ingest, tokio tasks, serde). The parts with genuine failure-mode risk are:

1. **The stdio control plane** — pipe deadlocks, partial lines, interleaved concurrent
   dispatches, children dying mid-write. This is the constraint: everything else composes
   around it, and its bugs are the kind that appear only under load or during crashes.
2. **Supervisor state machine** — the Starting/Ready/Backoff/Failed transitions interacting
   with SIGHUP rescans and in-flight requests. Second-order risk: races between "drain this
   ext" and "dispatch to this ext".

Everything else (pattern matching, config parsing, status output, the exts themselves) is
low-risk glue. **Therefore: build and harden the protocol + a fake-ext test harness first**,
before any HTTP exists. Elevating the constraint early means every later milestone tests
against a protocol layer that's already been abused.

Non-constraints, explicitly: performance (single node, webhook volumes are tiny by server
standards), the channel matcher (50 lines + property tests), and the real provider exts
(each is an afternoon once the dev-ext template exists).

## 2. Workspace layout

```
whdr/
├── Cargo.toml               # workspace
├── crates/
│   ├── whdr-proto/          # ExtMsg/SrvMsg/Event types, ndjson codec, pattern matcher — no IO
│   ├── whdr-server/         # daemon: ingest, supervisor, router, sockets
│   ├── whdr-cli/            # `whdr status` etc. (control-socket client)
│   ├── whdr-ext-kit/        # library for writing exts: register/dispatch loop, log helpers
│   └── whdr-ext-dev/        # echo/test extension built on ext-kit; doubles as the template
└── exts/
    ├── whdr-ext-github/
    └── whdr-ext-teams/
```

`whdr-proto` is the shared contract crate: server, ext-kit, and tests all depend on it, so a
schema change is one edit and the compiler finds every consumer. `whdr-ext-kit` exists so
each real ext is ~100 lines of provider logic, not 400 lines of stdio plumbing.

## 3. Milestones

Each milestone has a binary exit criterion — it's done or it isn't.

### M0 — Skeleton (S)
Workspace, CI (fmt/clippy/test), `tracing` setup, config + secrets-file parsing with the
0600 check, `--config` flag.
**Exit:** `whdr-server --config example.toml` starts, logs, and exits cleanly on SIGTERM.

### M1 — Protocol crate + harness ⚠ constraint (M)
`whdr-proto`: all message types, ndjson encode/decode (tolerant reader: skips blank lines,
surfaces malformed ones as typed errors), the NATS-grammar pattern matcher, channel/pattern
validators.
**Fake-ext harness:** an in-process "child" speaking the protocol over duplex pipes with
scriptable behaviors — respond slowly, respond out of order, emit garbage on stdout, die
mid-message, never register, flood events. This harness is the asset the whole plan leans on.
**Exit:** proto round-trip golden tests green; matcher property tests (proptest) green;
harness can express every misbehavior listed above.

### M2 — Dispatch pipeline end-to-end (M)
Ingest (axum) → path snapshot lookup → writer/reader task pair per child → req_id correlation
map with timeout → HTTP reply passthrough. Server-generated responses per SPEC §4.1 (404/503/
413/429/504). Body size limit, in-flight window.
**Exit:** `curl -d @payload.json localhost:8787/dev` returns the dev-ext's scripted reply;
harness-driven tests prove: out-of-order results correlate correctly; timeout → 504; late
Result after timeout is dropped; full in-flight window → 429.

### M3 — Supervisor lifecycle (M)
Scan, enable gate, spawn, register timeout, collision rejection, channel-namespace claims,
backoff respawn, crashloop → Failed, hang-detection kill, SIGHUP rescan with drain
(unroute → wait → Shutdown → TERM → KILL), clean whole-server shutdown.
**Exit:** integration test: start with ext A; SIGHUP after installing ext B → B live with
zero dropped in-flight requests to A; kill -9 a child 6× in a minute → Failed state visible;
hung child (harness: stop replying) is killed after 3 timeouts and comes back.

### M4 — Subscriber plane (WebSocket, multi-host) (M)
WebSocket listener on `sub_addr` (tokio-tungstenite), bearer-token handshake against the
tokens file, `welcome`/subscribe/unsubscribe/ping frames, per-connection bounded queue, drop
counters, fan-out from both `Result.events` and unsolicited `Event`s, WS ping liveness,
`closing` frame on shutdown/revoke. Plaintext-LAN guardrail (refuse non-loopback bind without
TLS or `allow_plaintext_lan`). Optional native rustls (`wss://`).
**Exit:** two test subscribers on *separate* connections with overlapping patterns each receive
exactly their matches; a bad/missing token is rejected at upgrade (401); revoking a token via
SIGHUP closes its live connection with `reason:"revoked"`; a stalled subscriber accrues
`dropped` while a healthy one loses nothing; disconnect cleans up `subs`/`conns` (asserted via
status); server refuses to start when `sub_addr` is non-loopback with neither TLS nor the
plaintext flag.

### M5 — Control socket + CLI (admin plane) (S–M)
Status JSON per SPEC §13; `whdr status` table + `--json`. **Token management:** control-socket
command set (`token.add`/`rotate`/`revoke`/`list`), daemon-side CSPRNG minting, SHA-256 hashing,
the atomic-replace store writer (tmp → fsync → rename, 0600 preserved), and immediate
connection-close on revoke/rotate. `whdr token …` subcommands.
**Exit:** status reflects a scripted scenario (1 Ready, 1 Failed, 2 subscribers, known drop
count) exactly. Token lifecycle test: `token add` prints a value that authenticates a live WS
connection; `token list` shows the name + fingerprint but never the value; `token rotate`
mints a new value, drops the old connection, and the old token now 401s; `token revoke` drops
the connection; **kill the server and restart → all surviving tokens still authenticate, the
revoked one still 401s** (persistence); a simulated crash mid-write (kill between tmp-write and
rename) leaves the prior store loadable.

### M6 — Real extensions (M)
`whdr-ext-kit` (register/dispatch loop, base64 + HMAC helpers), then:
`whdr-ext-github` — `X-Hub-Signature-256` HMAC over raw bytes, event → `github.<event>[.<action>]`;
`whdr-ext-teams` — `validationToken` echo handshake (`text/plain` reply headers), Graph
change-notification → `teams.<resource>` channels.
**Exit:** replayed captured real webhook payloads (fixtures) verify and emit the documented
channels; a tampered byte in the body fails verification with the ext's 401 passed through.

### M7 — Hardening + docs (M)
Chaos pass driven by the M1 harness across the whole stack (kill children during dispatch,
SIGHUP storms, subscriber churn under event load), fuzz the ndjson reader (cargo-fuzz),
README + ext-authoring guide (ext-kit walkthrough using whdr-ext-dev as the template),
example systemd unit.
**Exit:** chaos suite green in CI; a stranger can write a working ext from the guide alone.

**MVP = M0–M7.** Dependency chain is linear except M5 (can start after M3) and M6 (ext-kit
can start after M1; the exts need M2 to test against).

## 4. Test strategy

| Layer | Approach |
|-------|----------|
| Proto | Golden ndjson round-trips; fuzz the reader; proptest the matcher (∀ channel, `>` ⊇ exact; `*` matches exactly-one-token; invalid tokens rejected) |
| Dispatch/supervisor | Harness-driven integration tests, in-process (fast, deterministic) |
| Whole system | `assert_cmd`-style tests spawning the real server binary + real dev ext + real WS subscriber clients (with tokens); the M3/M4 exit scenarios live here, incl. auth-reject and revocation |
| Provider exts | Fixture payloads captured from real providers, incl. signature-failure cases |
| Chaos (M7) | Randomized fault injection using harness behaviors, seeded for reproducibility |

## 5. Risk register

| Risk | Exposure | Mitigation |
|------|----------|------------|
| stdio deadlock under load | Silent total stall | Dedicated reader/writer tasks + bounded channels from day one (SPEC §5.4); harness test that floods both directions |
| Child dies mid-line | Reader parses garbage / hangs | Tolerant reader in proto crate; EOF handling tested in M1, not discovered in M3 |
| Drain race (dispatch lands on draining ext) | Dropped webhook | Unroute-first ordering in SPEC §6.3; M3 exit test asserts zero drops during rescan |
| Secrets leak via logs | Security | `Dispatch` gets a manual `Debug` impl redacting `secret`; grep-CI check that no log call formats `SrvMsg` with derive-Debug |
| Scope creep toward durability | MVP never ships | Durable queue is explicitly post-MVP; the only concession made now is "drops are counted", which durability will reuse |
| Provider quirks (Teams handshake variants) | Ext rework | Fixtures from real traffic before writing the ext, not after |
| Token leaks via logs or plaintext LAN | Security | Tokens stored **hashed** (store leak ≠ credential leak); values never logged (redacted Debug); plaintext-LAN bind requires explicit flag; native/proxy TLS documented |
| Token-store corruption on write | Auth data loss | Atomic replace: tmp → fsync → rename, single writer through the command channel; M5 test kills mid-write and asserts the prior store still loads |
| WS backpressure vs. per-conn queue | Slow-consumer stall or unbounded memory | Bounded outbound queue + drop-count from M4; WS ping liveness catches dead-but-open sockets |

## 6. Post-MVP roadmap (ordered, not scheduled)

1. **Durable queue** — per-channel append log + per-subscriber offsets, TTL ~24 h; decide
   Redis vs. `redb` then, with "never persist secrets / consider payload encryption" as
   standing constraints. Per-subscriber offsets key off the token *name*, already available.
2. **Stronger subscriber auth** — mTLS client certs and/or per-token pattern scopes (a token
   restricted to `github.>`), layered on the existing named-token model without breaking it.
3. **Per-path secrets** — config schema already reserved.
4. **Raw-TCP subscriber transport** — same JSON messages for any consumer that can't speak WS
   (the runner-up in the D4 IGC); slots beside the WS listener.
5. **WASM ext boundary (Extism)** — only if third-party exts become real.
