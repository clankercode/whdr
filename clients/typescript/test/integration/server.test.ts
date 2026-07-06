/**
 * End-to-end integration against a REAL whdr-server (prebuilt debug binary).
 *
 * Drives the actual {@link WhdrSubscriber} client so these double as
 * conformance tests over the live wire, not just the mock. Covers: live
 * subscribe, resume-after-disconnect exactly-once replay, replay_gap on a
 * pruned floor, durability-disabled (error op replay) live-only, and bad-token
 * fatal auth.
 */
import { afterEach, describe, expect, test } from "vitest";

import {
  AuthError,
  MemoryCursorStore,
  WhdrSubscriber,
  type DeliveredEvent,
  type Handler,
} from "../../src/index.js";
import {
  binariesAvailable,
  missingBinariesMessage,
  WhdrServer,
} from "./harness.js";
import { statSync } from "node:fs";

const fastBackoff = { initialMs: 10, maxMs: 50, multiplier: 2, jitter: 0 };
let server: WhdrServer | undefined;

afterEach(async () => {
  await server?.stop();
  server = undefined;
});

/**
 * Run `sub` until `handler` (wrapped) signals completion via `stop`, then abort
 * and resolve. Collects delivered events. Fatal errors reject.
 */
function runUntil(
  sub: WhdrSubscriber,
  build: (events: DeliveredEvent[], stop: () => void) => Handler,
): Promise<DeliveredEvent[]> {
  const events: DeliveredEvent[] = [];
  const ctrl = new AbortController();
  const stop = (): void => ctrl.abort();
  return new Promise<DeliveredEvent[]>((resolve, reject) => {
    const handler = build(events, () => {
      stop();
      resolve(events);
    });
    sub.run(handler, { signal: ctrl.signal }).then(
      () => resolve(events),
      (err) => reject(err),
    );
  });
}

describe.skipIf(!binariesAvailable())("whdr-server integration", () => {
  if (!binariesAvailable()) {
    // Surfaced as a skip reason in output.
    test.skip(missingBinariesMessage(), () => {});
  }

  test("live subscribe delivers a fanned-out event (checklist 2, 9)", async () => {
    server = await WhdrServer.start({ delivery: { extraLines: "" } });
    const token = server.tokenAdd("live");
    const sub = new WhdrSubscriber({
      url: server.subUrl,
      token,
      patterns: [`${server.extId}.>`],
      cursor: 0,
      backoff: fastBackoff,
    });

    const events: DeliveredEvent[] = [];
    const ctrl = new AbortController();
    await new Promise<void>((resolve, reject) => {
      sub
        .run(
          {
            onEvent: (e) => {
              events.push(e);
              ctrl.abort();
              resolve();
            },
            // Nothing stored yet, so replay finishes immediately; emit live.
            onReplayed: () => {
              void server!.emit("hello-live");
            },
          },
          { signal: ctrl.signal },
        )
        .catch(reject);
    });

    expect(events).toHaveLength(1);
    expect(events[0]!.channel).toBe(`${server.extId}.echo`);
    expect(Buffer.from(events[0]!.payload()).toString("utf8")).toBe("hello-live");
  });

  test("resume after disconnect replays missed events exactly-once (checklist 3, 4, 5)", async () => {
    server = await WhdrServer.start({ delivery: { extraLines: "" } });

    // Emit two events with NO subscriber connected: persisted seq 1, 2.
    expect(await server.emit("one")).toBe(200);
    expect(await server.emit("two")).toBe(200);

    const token = server.tokenAdd("resumer");
    const store = new MemoryCursorStore(0);
    const patterns = [`${server.extId}.>`];

    // First session: replay from 0, receive seq 1 & 2, then disconnect.
    const first = await runUntil(
      new WhdrSubscriber({ url: server.subUrl, token, patterns, cursorStore: store, backoff: fastBackoff }),
      (events, done) => ({
        onEvent: (e) => {
          events.push(e);
        },
        onReplayed: (through) => {
          if (through >= 2) done();
        },
      }),
    );
    expect(first.map((e) => e.seq)).toEqual([1, 2]);
    expect(store.get()).toBe(2); // cursor persisted after handling

    // While disconnected, emit a third event: persisted seq 3.
    expect(await server.emit("three")).toBe(200);

    // Second session: resume from the persisted cursor (2). Only seq 3 replays
    // — exactly-once, no re-delivery of 1/2.
    const second = await runUntil(
      new WhdrSubscriber({ url: server.subUrl, token, patterns, cursorStore: store, backoff: fastBackoff }),
      (events, done) => ({
        onEvent: (e) => {
          events.push(e);
        },
        onReplayed: (through) => {
          if (through >= 3) done();
        },
      }),
    );
    expect(second.map((e) => e.seq)).toEqual([3]);
    expect(store.get()).toBe(3);

    // Store file is 0600 at rest ([D-dursec]).
    const mode = statSync(server.storePath!).mode & 0o777;
    expect(mode).toBe(0o600);
  });

  test("replay below the retained floor surfaces replay_gap (checklist 7)", async () => {
    // Cap retention to a single event with fast pruning so the floor rises.
    server = await WhdrServer.start({
      delivery: { extraLines: "max_events = 1\nprune_interval_secs = 1" },
    });

    for (const body of ["a", "b", "c", "d", "e"]) {
      expect(await server.emit(body)).toBe(200);
    }

    const token = server.tokenAdd("gapper");
    const patterns = [`${server.extId}.>`];

    // Poll: once the background prune raises the floor, resuming from an old
    // cursor (after_seq=1) yields an explicit replay_gap.
    const deadline = Date.now() + 15_000;
    let gap: [number, number] | undefined;
    for (;;) {
      const seen: [number, number][] = [];
      const events = await runUntil(
        new WhdrSubscriber({ url: server.subUrl, token, patterns, cursor: 1, backoff: fastBackoff }),
        (evs, done) => ({
          onEvent: (e) => {
            evs.push(e);
          },
          onReplayGap: (from, earliest) => {
            seen.push([from, earliest]);
          },
          onReplayed: () => done(),
        }),
      );
      if (seen.length > 0) {
        gap = seen[0];
        // After the gap, replay resumes from the floor (seq 5 survives).
        expect(events.at(-1)!.seq).toBe(5);
        break;
      }
      if (Date.now() > deadline) throw new Error("background prune never raised the floor");
      await new Promise((r) => setTimeout(r, 300));
    }
    expect(gap).toEqual([1, 5]);
  });

  test("durability disabled: replay refused (error op replay), live continues (checklist 7 sibling)", async () => {
    server = await WhdrServer.start({ delivery: false });
    const token = server.tokenAdd("liveonly");
    const sub = new WhdrSubscriber({
      url: server.subUrl,
      token,
      patterns: [`${server.extId}.>`],
      cursor: 0, // sends replay.after_seq=0; server refuses
      backoff: fastBackoff,
    });

    let refusedMsg: string | undefined;
    const events: DeliveredEvent[] = [];
    const ctrl = new AbortController();
    await new Promise<void>((resolve, reject) => {
      sub
        .run(
          {
            onEvent: (e) => {
              events.push(e);
              ctrl.abort();
              resolve();
            },
            onReplayUnavailable: (msg) => {
              refusedMsg = msg;
              // Live still works: emit and expect delivery.
              void server!.emit("live-after-refusal");
            },
          },
          { signal: ctrl.signal },
        )
        .catch(reject);
    });

    expect(refusedMsg).toContain("not enabled");
    expect(events).toHaveLength(1);
    expect(Buffer.from(events[0]!.payload()).toString("utf8")).toBe("live-after-refusal");
    // Disabled path creates no store file.
    expect(server.storePath).toBeNull();
  });

  test("bad token is fatal — run rejects with AuthError (checklist 1)", async () => {
    server = await WhdrServer.start({ delivery: false });
    const sub = new WhdrSubscriber({
      url: server.subUrl,
      token: "tok_definitely_wrong",
      patterns: [`${server.extId}.>`],
      backoff: fastBackoff,
    });
    await expect(sub.run({ onEvent: () => {} })).rejects.toBeInstanceOf(AuthError);
  });
});
