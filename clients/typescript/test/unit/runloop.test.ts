import { afterEach, describe, expect, test } from "vitest";

import {
  AuthError,
  cursorStoreFromCallbacks,
  HandlerError,
  RevokedError,
  WhdrSubscriber,
} from "../../src/index.js";
import { MockServer } from "./support/mock-server.js";

const TOKEN = "tok_test";
let server: MockServer | undefined;

afterEach(async () => {
  await server?.stop();
  server = undefined;
});

const fastBackoff = { initialMs: 5, maxMs: 20, multiplier: 2, jitter: 0 };
const uuid = (n: number): string => `00000000-0000-0000-0000-${String(n).padStart(12, "0")}`;

function subscriber(url: string, extra: Record<string, unknown> = {}): WhdrSubscriber {
  return new WhdrSubscriber({
    url,
    token: TOKEN,
    patterns: ["dev.>"],
    backoff: fastBackoff,
    rand: () => 0.5,
    ...extra,
  });
}

describe("run loop — reconnect and fatality (conformance items 6, 8)", () => {
  test("lagged triggers reconnect and resume from the cursor (item 6)", async () => {
    const subscribeCursors: number[] = [];
    server = await MockServer.start({
      token: TOKEN,
      onConnection: (conn, index) => {
        conn.send({ type: "welcome", name: "p" });
        conn.waitFor((f) => (f as { type?: string }).type === "subscribe").then((f) => {
          subscribeCursors.push((f as { replay?: { after_seq: number } }).replay?.after_seq ?? -1);
          conn.send({ type: "ok", op: "subscribe" });
          if (index === 0) {
            conn.send({ type: "event", id: uuid(1), seq: 1, ts_ms: 1, channel: "dev.a", payload_b64: "AA==" });
            conn.send({ type: "lagged", dropped: 3 });
          } else {
            // Reconnect: replay from cursor delivers the missed event.
            conn.send({ type: "event", id: uuid(2), seq: 2, ts_ms: 1, channel: "dev.b", payload_b64: "AA==" });
          }
        });
      },
    });

    const seqs: number[] = [];
    let laggedDropped = -1;
    const ctrl = new AbortController();
    const sub = subscriber(server.url, { cursor: 0 });
    await new Promise<void>((resolve) => {
      void sub.run(
        {
          onEvent: (e) => {
            seqs.push(e.seq);
            if (e.seq === 2) {
              ctrl.abort();
              resolve();
            }
          },
          onLagged: (dropped) => {
            laggedDropped = dropped;
          },
        },
        { signal: ctrl.signal },
      );
    });

    expect(laggedDropped).toBe(3);
    expect(seqs).toEqual([1, 2]);
    // First connect resumed from 0, reconnect resumed from cursor=1 (item 3/6).
    expect(subscribeCursors).toEqual([0, 1]);
  });

  test("closing 'shutdown' reconnects with backoff (item 8)", async () => {
    let connections = 0;
    server = await MockServer.start({
      token: TOKEN,
      onConnection: (conn, index) => {
        connections = index + 1;
        conn.send({ type: "welcome", name: "p" });
        conn.waitFor((f) => (f as { type?: string }).type === "subscribe").then(() => {
          conn.send({ type: "ok", op: "subscribe" });
          if (index === 0) {
            conn.send({ type: "closing", reason: "shutdown" });
          } else {
            conn.send({ type: "event", id: uuid(1), seq: 1, ts_ms: 1, channel: "dev.a", payload_b64: "AA==" });
          }
        });
      },
    });

    const ctrl = new AbortController();
    const sub = subscriber(server.url, { cursor: 0 });
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
    expect(connections).toBeGreaterThanOrEqual(2);
  });

  test("closing 'revoked' is fatal — run rejects with RevokedError (item 8)", async () => {
    server = await MockServer.start({
      token: TOKEN,
      onConnection: (conn) => {
        conn.send({ type: "welcome", name: "p" });
        conn.waitFor((f) => (f as { type?: string }).type === "subscribe").then(() => {
          conn.send({ type: "ok", op: "subscribe" });
          conn.send({ type: "closing", reason: "revoked" });
        });
      },
    });
    const sub = subscriber(server.url, { cursor: 0 });
    await expect(sub.run({ onEvent: () => {} })).rejects.toBeInstanceOf(RevokedError);
  });

  test("a dropped socket reconnects and resumes (item 6)", async () => {
    let connections = 0;
    server = await MockServer.start({
      token: TOKEN,
      onConnection: (conn, index) => {
        connections = index + 1;
        conn.send({ type: "welcome", name: "p" });
        conn.waitFor((f) => (f as { type?: string }).type === "subscribe").then(() => {
          conn.send({ type: "ok", op: "subscribe" });
          if (index === 0) {
            // Hard drop with no closing frame.
            conn.socket.terminate();
          } else {
            conn.send({ type: "event", id: uuid(1), seq: 1, ts_ms: 1, channel: "dev.a", payload_b64: "AA==" });
          }
        });
      },
    });

    const ctrl = new AbortController();
    const sub = subscriber(server.url, { cursor: 0 });
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
    expect(connections).toBeGreaterThanOrEqual(2);
  });
});

describe("run loop — auth, persistence, handler, replay-unavailable", () => {
  test("HTTP 401 is fatal — run rejects with AuthError (conformance item 1)", async () => {
    server = await MockServer.start({
      token: "tok_good",
      onConnection: (conn) => conn.send({ type: "welcome", name: "p" }),
    });
    const sub = new WhdrSubscriber({
      url: server.url,
      token: "tok_wrong",
      patterns: ["dev.>"],
      backoff: fastBackoff,
    });
    await expect(sub.run({ onEvent: () => {} })).rejects.toBeInstanceOf(AuthError);
  });

  test("persists the cursor after each successful handle (conformance item 5)", async () => {
    server = await MockServer.start({
      token: TOKEN,
      onConnection: (conn) => {
        conn.send({ type: "welcome", name: "p" });
        conn.waitFor((f) => (f as { type?: string }).type === "subscribe").then(() => {
          conn.send({ type: "ok", op: "subscribe" });
          conn.send({ type: "event", id: uuid(1), seq: 1, ts_ms: 1, channel: "dev.a", payload_b64: "AA==" });
          conn.send({ type: "event", id: uuid(2), seq: 2, ts_ms: 1, channel: "dev.b", payload_b64: "AA==" });
        });
      },
    });

    const saves: number[] = [];
    let stored = 0;
    const url = server.url;
    const ctrl = new AbortController();
    const done = new Promise<void>((resolve) => {
      const store = cursorStoreFromCallbacks({
        load: () => stored,
        save: (c) => {
          stored = c;
          saves.push(c);
          // Resolve on the save that follows the last handled event, so we
          // assert only after the cursor has actually been persisted.
          if (c === 2) {
            ctrl.abort();
            resolve();
          }
        },
      });
      const sub = subscriber(url, { cursorStore: store });
      void sub.run({ onEvent: () => {} }, { signal: ctrl.signal });
    });
    await done;
    // Cursor persisted only after handling, monotonically.
    expect(saves).toEqual([1, 2]);
  });

  test("a throwing handler is fatal — run rejects with HandlerError", async () => {
    server = await MockServer.start({
      token: TOKEN,
      onConnection: (conn) => {
        conn.send({ type: "welcome", name: "p" });
        conn.waitFor((f) => (f as { type?: string }).type === "subscribe").then(() => {
          conn.send({ type: "ok", op: "subscribe" });
          conn.send({ type: "event", id: uuid(1), seq: 1, ts_ms: 1, channel: "dev.a", payload_b64: "AA==" });
        });
      },
    });
    const sub = subscriber(server.url, { cursor: 0 });
    await expect(
      sub.run({
        onEvent: () => {
          throw new Error("boom");
        },
      }),
    ).rejects.toBeInstanceOf(HandlerError);
  });

  test("error op 'replay' is surfaced as replay-unavailable; live continues (item 7 sibling)", async () => {
    server = await MockServer.start({
      token: TOKEN,
      onConnection: (conn) => {
        conn.send({ type: "welcome", name: "p" });
        conn.waitFor((f) => (f as { type?: string }).type === "subscribe").then(() => {
          conn.send({ type: "ok", op: "subscribe" });
          conn.send({ type: "error", op: "replay", msg: "durable delivery is not enabled" });
          conn.send({ type: "event", id: uuid(1), seq: 1, ts_ms: 1, channel: "dev.a", payload_b64: "AA==" });
        });
      },
    });

    let replayMsg: string | undefined;
    const ctrl = new AbortController();
    const sub = subscriber(server.url, { cursor: 0 });
    await new Promise<void>((resolve) => {
      void sub.run(
        {
          onEvent: () => {
            ctrl.abort();
            resolve();
          },
          onReplayUnavailable: (msg) => {
            replayMsg = msg;
          },
        },
        { signal: ctrl.signal },
      );
    });
    expect(replayMsg).toContain("not enabled");
  });

  test("stream() yields de-duplicated events via async iteration", async () => {
    server = await MockServer.start({
      token: TOKEN,
      onConnection: (conn) => {
        conn.send({ type: "welcome", name: "p" });
        conn.waitFor((f) => (f as { type?: string }).type === "subscribe").then(() => {
          conn.send({ type: "ok", op: "subscribe" });
          conn.send({ type: "event", id: uuid(1), seq: 1, ts_ms: 1, channel: "dev.a", payload_b64: "AA==" });
          conn.send({ type: "event", id: uuid(2), seq: 2, ts_ms: 1, channel: "dev.b", payload_b64: "AA==" });
        });
      },
    });

    const sub = subscriber(server.url, { cursor: 0 });
    const iterator = sub.stream();
    const seqs: number[] = [];
    for await (const event of iterator) {
      seqs.push(event.seq);
      if (event.seq === 2) {
        await iterator.return?.();
        break;
      }
    }
    expect(seqs).toEqual([1, 2]);
  });
});
