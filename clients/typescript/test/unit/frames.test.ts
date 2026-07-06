import { describe, expect, test } from "vitest";

import { parseServerFrame, subscribeFrame } from "../../src/frames.js";

describe("parseServerFrame", () => {
  test("parses a known event frame (conformance item 4/5 fields)", () => {
    const text = JSON.stringify({
      type: "event",
      id: "00000000-0000-0000-0000-000000000000",
      seq: 7,
      ts_ms: 1_751_760_000_000,
      channel: "github.push",
      payload_b64: "AA==",
    });
    const frame = parseServerFrame(text);
    expect(frame).toEqual({
      type: "event",
      id: "00000000-0000-0000-0000-000000000000",
      seq: 7,
      ts_ms: 1_751_760_000_000,
      channel: "github.push",
      payload_b64: "AA==",
    });
  });

  test("skips unknown frame type (conformance item 10)", () => {
    expect(parseServerFrame(JSON.stringify({ type: "quantum_flux", foo: 1 }))).toBeNull();
    expect(parseServerFrame("not json at all")).toBeNull();
    expect(parseServerFrame(JSON.stringify([1, 2, 3]))).toBeNull();
  });

  test("tolerates unknown extra fields on a known frame (conformance item 10)", () => {
    const text = JSON.stringify({ type: "welcome", name: "p", future_field: { nested: true } });
    expect(parseServerFrame(text)).toEqual({ type: "welcome", name: "p" });
  });

  test("rejects a known frame missing a required field", () => {
    // seq missing => not a usable event.
    expect(
      parseServerFrame(
        JSON.stringify({ type: "event", id: "x", ts_ms: 1, channel: "c", payload_b64: "AA==" }),
      ),
    ).toBeNull();
  });

  test("parses replayed / replay_gap / lagged control frames", () => {
    expect(parseServerFrame(JSON.stringify({ type: "replayed", through_seq: 128 }))).toEqual({
      type: "replayed",
      through_seq: 128,
    });
    expect(
      parseServerFrame(JSON.stringify({ type: "replay_gap", from_seq: 10, earliest_seq: 57 })),
    ).toEqual({ type: "replay_gap", from_seq: 10, earliest_seq: 57 });
    expect(parseServerFrame(JSON.stringify({ type: "lagged", dropped: 42 }))).toEqual({
      type: "lagged",
      dropped: 42,
    });
  });

  test("parses closing with both reasons; rejects an unknown reason", () => {
    expect(parseServerFrame(JSON.stringify({ type: "closing", reason: "shutdown" }))).toEqual({
      type: "closing",
      reason: "shutdown",
    });
    expect(parseServerFrame(JSON.stringify({ type: "closing", reason: "revoked" }))).toEqual({
      type: "closing",
      reason: "revoked",
    });
    expect(parseServerFrame(JSON.stringify({ type: "closing", reason: "aliens" }))).toBeNull();
  });

  test("parses ok / error / pong", () => {
    expect(parseServerFrame(JSON.stringify({ type: "ok", op: "subscribe" }))).toEqual({
      type: "ok",
      op: "subscribe",
    });
    expect(
      parseServerFrame(JSON.stringify({ type: "error", op: "replay", msg: "not enabled" })),
    ).toEqual({ type: "error", op: "replay", msg: "not enabled" });
    expect(parseServerFrame(JSON.stringify({ type: "pong" }))).toEqual({ type: "pong" });
  });
});

describe("subscribeFrame", () => {
  test("attaches replay.after_seq = cursor when given (conformance item 3)", () => {
    expect(subscribeFrame(["github.>"], 128)).toEqual({
      type: "subscribe",
      patterns: ["github.>"],
      replay: { after_seq: 128 },
    });
  });

  test("omits replay for live-only", () => {
    const frame = subscribeFrame(["github.>"], undefined);
    expect(frame).toEqual({ type: "subscribe", patterns: ["github.>"] });
    expect("replay" in frame).toBe(false);
  });

  test("resume cursor 0 still sends replay (cold-start replay from retention)", () => {
    expect(subscribeFrame(["a.>"], 0)).toEqual({
      type: "subscribe",
      patterns: ["a.>"],
      replay: { after_seq: 0 },
    });
  });
});
