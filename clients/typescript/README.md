# whdr-sub-kit (TypeScript)

TypeScript client library for the **whdr** subscriber plane — the *Subscriber
wire protocol v2* (durable delivery / replay). It mirrors the behaviour of the
reference Rust crate [`whdr-sub-kit`](../../crates/whdr-sub-kit); see
`docs/SPEC.md` §9/§9.4 and `docs/SUBSCRIBERS.md`.

whdr fans provider-webhook events out to token-authenticated WebSocket
subscribers. With durable delivery enabled on the server, a subscriber can
**resume from a cursor** and replay events it missed while offline or after a
slow-consumer drop — at-least-once, de-duplicated by event `id`.

- ESM, TypeScript strict mode.
- Runtime dependency: [`ws`](https://www.npmjs.com/package/ws) only.

## Install

```bash
npm install   # then: npm run build
```

## Quickstart — the `run` loop

`WhdrSubscriber.run(handler)` implements the full appendix §7 reconnect-and-resume
algorithm for you: auth → `welcome` → subscribe with `replay.after_seq = cursor`
→ dedup by `id`/`seq` → advance & persist the cursor after each successful
handle → recover from `lagged`/dropped sockets by reconnecting from the cursor →
surface `replay_gap` → treat `revoked` as fatal and `shutdown` as a backoff
reconnect → ignore unknown frames.

```ts
import { WhdrSubscriber } from "whdr-sub-kit";

const sub = new WhdrSubscriber({
  url: "ws://127.0.0.1:8788/subscribe",
  token: process.env.WHDR_TOKEN!,
  patterns: ["github.>"],   // NATS-style dotted subjects
  cursor: 0,                 // 0 = replay from the start of retention
});

// Runs forever, reconnecting with exponential backoff + jitter.
// Rejects only on a fatal error: auth/revoked/handler/cursor-store failure.
await sub.run({
  async onEvent(event) {
    const body = event.payload();               // base64-decoded Uint8Array
    console.log(`seq=${event.seq} ${event.channel} ${body.length} bytes`);
  },
  onReplayGap(from, earliest) {                  // explicit, permanent data loss
    console.error(`replay_gap: events (${from}, ${earliest}) were pruned`);
  },
});
```

The cursor advances (and is persisted) **only after** `onEvent` resolves —
at-least-once. Throwing from any hook is fatal and stops the loop.

### Persisting the cursor across restarts

Pass a `cursorStore` (async `load`/`save`); `save` is called after each handled
event, `load` supplies the starting cursor.

```ts
import { WhdrSubscriber, cursorStoreFromCallbacks } from "whdr-sub-kit";
import { readFile, writeFile } from "node:fs/promises";

const cursorStore = cursorStoreFromCallbacks({
  load: async () => Number(await readFile("cursor", "utf8").catch(() => "0")),
  save: (c) => writeFile("cursor", String(c)),
});
new WhdrSubscriber({ url, token, patterns: ["github.>"], cursorStore });
```

### Async-iterator API

```ts
const events = sub.stream({ onReplayGap: (f, e) => console.warn(f, e) });
for await (const event of events) {
  handle(event);
}
await events.return?.();   // stop the loop
```

### Bespoke loop

`sub.connect()` returns a `Connection` positioned just after `welcome`; drive
`connection.recv()` yourself (a typed `ServerFrame` stream that auto-answers WS
pings and skips unknown frames) and apply `ResumeState` for the dedup/cursor
guard.

## Configuration

| Option | Default | Meaning |
|--------|---------|---------|
| `url` | — | `/subscribe` endpoint (`ws://…` / `wss://…`). |
| `token` | — | `tok_…` subscriber token minted by the operator. |
| `patterns` | — | NATS-style channel patterns. |
| `cursor` | `0` | Initial resume cursor (ignored if `cursorStore` set). |
| `cursorStore` | in-memory | Cross-restart cursor persistence hook. |
| `backoff` | 500ms→30s, ×2, ±20% | Exponential-backoff-with-jitter policy. |
| `dedupCapacity` | `8192` | Recent-`id` dedup window size. |
| `wsOptions` | `{}` | Extra `ws` client options (e.g. TLS). |

## Testing

```bash
npm run test:unit          # pure logic + mock-WS protocol-order tests
npm run test:integration   # boots the real whdr-server debug binary
npm test                   # both
```

Unit tests use an in-process `ws` server to script exact frame orderings
(including the `ok`-before-`replay_gap` case). Integration tests boot the real
`whdr-server` from `target/debug/` (override with `WHDR_SERVER_BIN`,
`WHDR_CLI_BIN`, `WHDR_FAKE_EXT_BIN`, `WHDR_REPO_ROOT`); they self-skip if the
binaries are absent.

## Conformance map

The appendix's 10-point client-library conformance checklist, each item mapped
to the test(s) that cover it:

| # | Checklist item | Test(s) |
|---|----------------|---------|
| 1 | `Authorization: Bearer` on upgrade; `401` fatal | `runloop.test.ts` › *HTTP 401 is fatal — AuthError*; `server.test.ts` › *bad token is fatal — AuthError* |
| 2 | Waits for `welcome` before subscribing | `protocol-order.test.ts` › *waits for welcome before sending subscribe* |
| 3 | Sends `subscribe` with `replay.after_seq = cursor` on every (re)connect | `protocol-order.test.ts` › *subscribe carries replay.after_seq = cursor*; `runloop.test.ts` › *lagged triggers reconnect and resume from the cursor* (asserts cursors `[0, 1]`) |
| 4 | Dedups by `id`; ignores `seq <= cursor` | `resume.test.ts` › *skips seq at or below the cursor*, *dedups by id across the replay/live boundary*; `protocol-order.test.ts` › *dedups a duplicate id across the replay/live boundary* |
| 5 | Advances & (optionally) persists cursor only after handling | `resume.test.ts` › *cursor advances only via record*; `runloop.test.ts` › *persists the cursor after each successful handle* |
| 6 | `lagged` and `<ws error>` → reconnect + resume from cursor | `runloop.test.ts` › *lagged triggers reconnect and resume from the cursor*, *a dropped socket reconnects and resumes* |
| 7 | Treats `replay_gap` as explicit, logged data-loss | `server.test.ts` › *replay below the retained floor surfaces replay_gap*; `protocol-order.test.ts` › *ok BEFORE replay_gap …* |
| 8 | Handles `closing` (`revoked` → renew/fatal; `shutdown` → backoff reconnect) | `runloop.test.ts` › *closing 'revoked' is fatal*, *closing 'shutdown' reconnects with backoff* |
| 9 | Answers WebSocket ping frames | `protocol-order.test.ts` › *answers WebSocket ping frames automatically* |
| 10 | Ignores unknown frame `type`s and unknown fields | `frames.test.ts` › *skips unknown frame type*, *tolerates unknown extra fields*; `protocol-order.test.ts` › *ignores unknown frame types and malformed frames inline* |

Plus the four documented protocol subtleties: `ok`-before-`replay_gap` ordering
(order-agnostic frame handling), `replay_gap`'s `earliest_seq` being delivered
(only strictly-interior events lost), matching on `error.op == "replay"` (never
`msg` text), and cursor = highest seq processed with `after_seq` exclusive — all
exercised by the tests above and the integration replay tests.
