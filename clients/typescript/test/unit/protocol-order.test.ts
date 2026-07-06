import { afterEach, describe, expect, test } from "vitest";

import { WhdrSubscriber, type DeliveredEvent } from "../../src/index.js";
import { MockServer } from "./support/mock-server.js";

const TOKEN = "tok_test";
let server: MockServer | undefined;

afterEach(async () => {
  await server?.stop();
  server = undefined;
});

/** Fast, deterministic backoff so reconnect tests don't dawdle. */
const fastBackoff = { initialMs: 5, maxMs: 20, multiplier: 2, jitter: 0 };

function newSubscriber(url: string, extra: Record<string, unknown> = {}): WhdrSubscriber {
  return new WhdrSubscriber({
    url,
    token: TOKEN,
    patterns: ["dev.>"],
    backoff: fastBackoff,
    rand: () => 0.5,
    ...extra,
  });
}

const uuid = (n: number): string => `00000000-0000-0000-0000-${String(n).padStart(12, "0")}`;

describe("replay/live protocol ordering", () => {
  test("ok BEFORE replay_gap, then events, then replayed — order-agnostic (subtlety 1)", async () => {
    server = await MockServer.start({
      token: TOKEN,
      onConnection: (conn) => {
        conn.send({ type: "welcome", name: "p" });
        conn.waitFor((f) => (f as { type?: string }).type === "subscribe").then(() => {
          // Reference server order: ok first, THEN replay_gap, THEN replayed events.
          conn.send({ type: "ok", op: "subscribe" });
          conn.send({ type: "replay_gap", from_seq: 1, earliest_seq: 5 });
          conn.send({ type: "event", id: uuid(5), seq: 5, ts_ms: 1, channel: "dev.a", payload_b64: "AA==" });
          conn.send({ type: "event", id: uuid(6), seq: 6, ts_ms: 1, channel: "dev.b", payload_b64: "AQ==" });
          conn.send({ type: "replayed", through_seq: 6 });
        });
      },
    });

    const events: DeliveredEvent[] = [];
    const gaps: Array<[number, number]> = [];
    let replayedThrough = -1;
    const ctrl = new AbortController();
    const sub = newSubscriber(server.url, { cursor: 0 });
    const done = new Promise<void>((resolve) => {
      void sub.run(
        {
          onEvent: (e) => {
            events.push(e);
          },
          onReplayGap: (from, earliest) => {
            gaps.push([from, earliest]);
          },
          onReplayed: (through) => {
            // `replayed` is the last frame the server sends here.
            replayedThrough = through;
            ctrl.abort();
            resolve();
          },
        },
        { signal: ctrl.signal },
      );
    });
    await done;

    expect(gaps).toEqual([[1, 5]]);
    expect(events.map((e) => e.seq)).toEqual([5, 6]);
    expect(replayedThrough).toBe(6);
    // earliest_seq (5) itself IS delivered — only strictly-interior events lost (subtlety 2).
    expect(events[0]!.seq).toBe(5);
  });

  test("dedups a duplicate id across the replay/live boundary (subtlety in D-dedup)", async () => {
    server = await MockServer.start({
      token: TOKEN,
      onConnection: (conn) => {
        conn.send({ type: "welcome", name: "p" });
        conn.waitFor((f) => (f as { type?: string }).type === "subscribe").then(() => {
          conn.send({ type: "ok", op: "subscribe" });
          conn.send({ type: "event", id: uuid(1), seq: 1, ts_ms: 1, channel: "dev.a", payload_b64: "AA==" });
          conn.send({ type: "event", id: uuid(2), seq: 2, ts_ms: 1, channel: "dev.b", payload_b64: "AA==" });
          conn.send({ type: "replayed", through_seq: 2 });
          // Live: id(2)/seq 2 repeats (replay/live overlap), then a fresh id(3).
          conn.send({ type: "event", id: uuid(2), seq: 2, ts_ms: 1, channel: "dev.b", payload_b64: "AA==" });
          conn.send({ type: "event", id: uuid(3), seq: 3, ts_ms: 1, channel: "dev.c", payload_b64: "AA==" });
        });
      },
    });

    const seen: string[] = [];
    const ctrl = new AbortController();
    const sub = newSubscriber(server.url, { cursor: 0 });
    await new Promise<void>((resolve) => {
      void sub.run(
        {
          onEvent: (e) => {
            seen.push(e.id);
            if (e.seq === 3) {
              ctrl.abort();
              resolve();
            }
          },
        },
        { signal: ctrl.signal },
      );
    });

    // id(2) handed to the handler exactly once despite two deliveries.
    expect(seen).toEqual([uuid(1), uuid(2), uuid(3)]);
  });

  test("ignores unknown frame types and malformed frames inline (conformance item 10)", async () => {
    server = await MockServer.start({
      token: TOKEN,
      onConnection: (conn) => {
        conn.send({ type: "welcome", name: "p" });
        conn.waitFor((f) => (f as { type?: string }).type === "subscribe").then(() => {
          conn.send({ type: "ok", op: "subscribe" });
          conn.send({ type: "quantum_flux", weird: true }); // unknown type
          conn.sendRaw("this is not json"); // malformed
          conn.send({ type: "event", id: uuid(1), seq: 1, ts_ms: 1, channel: "dev.a", payload_b64: "aGk=" });
        });
      },
    });

    const ctrl = new AbortController();
    const sub = newSubscriber(server.url, { cursor: 0 });
    const payloads: string[] = [];
    await new Promise<void>((resolve) => {
      void sub.run(
        {
          onEvent: (e) => {
            payloads.push(Buffer.from(e.payload()).toString("utf8"));
            ctrl.abort();
            resolve();
          },
        },
        { signal: ctrl.signal },
      );
    });
    expect(payloads).toEqual(["hi"]);
  });

  test("subscribe carries replay.after_seq = cursor on connect (conformance item 3)", async () => {
    let subscribeFrame: unknown;
    server = await MockServer.start({
      token: TOKEN,
      onConnection: (conn) => {
        conn.send({ type: "welcome", name: "p" });
        conn.waitFor((f) => (f as { type?: string }).type === "subscribe").then((f) => {
          subscribeFrame = f;
          conn.send({ type: "ok", op: "subscribe" });
          conn.send({ type: "event", id: uuid(43), seq: 43, ts_ms: 1, channel: "dev.a", payload_b64: "AA==" });
        });
      },
    });

    const ctrl = new AbortController();
    const sub = newSubscriber(server.url, { cursor: 42 });
    await new Promise<void>((resolve) => {
      void sub.run(
        {
          onEvent: () => {
            ctrl.abort();
            resolve();
          },
        },
        { signal: ctrl.signal },
      );
    });
    expect(subscribeFrame).toEqual({
      type: "subscribe",
      patterns: ["dev.>"],
      replay: { after_seq: 42 },
    });
  });

  test("waits for welcome before sending subscribe (conformance item 2)", async () => {
    let subscribeSeenBeforeWelcome = false;
    server = await MockServer.start({
      token: TOKEN,
      onConnection: (conn) => {
        // Delay welcome; a conformant client must not subscribe until it lands.
        setTimeout(() => {
          subscribeSeenBeforeWelcome = conn.received.some(
            (f) => (f as { type?: string }).type === "subscribe",
          );
          conn.send({ type: "welcome", name: "p" });
          conn.waitFor((f) => (f as { type?: string }).type === "subscribe").then(() => {
            conn.send({ type: "ok", op: "subscribe" });
            conn.send({ type: "event", id: uuid(1), seq: 1, ts_ms: 1, channel: "dev.a", payload_b64: "AA==" });
          });
        }, 100);
      },
    });

    const ctrl = new AbortController();
    const sub = newSubscriber(server.url, { cursor: 0 });
    await new Promise<void>((resolve) => {
      void sub.run(
        {
          onEvent: () => {
            ctrl.abort();
            resolve();
          },
        },
        { signal: ctrl.signal },
      );
    });
    expect(subscribeSeenBeforeWelcome).toBe(false);
  });

  test("answers WebSocket ping frames automatically (conformance item 9)", async () => {
    let pongReceived: Promise<void> | undefined;
    server = await MockServer.start({
      token: TOKEN,
      onConnection: (conn) => {
        conn.send({ type: "welcome", name: "p" });
        conn.waitFor((f) => (f as { type?: string }).type === "subscribe").then(() => {
          conn.send({ type: "ok", op: "subscribe" });
          pongReceived = conn.waitForPong();
          conn.pingWs("liveness");
          void pongReceived.then(() => {
            conn.send({ type: "event", id: uuid(1), seq: 1, ts_ms: 1, channel: "dev.a", payload_b64: "AA==" });
          });
        });
      },
    });

    const ctrl = new AbortController();
    const sub = newSubscriber(server.url, { cursor: 0 });
    await new Promise<void>((resolve) => {
      void sub.run(
        {
          onEvent: () => {
            ctrl.abort();
            resolve();
          },
        },
        { signal: ctrl.signal },
      );
    });
    await expect(pongReceived).resolves.toBeUndefined();
  });
});
