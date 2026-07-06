/**
 * Typed wire frames for the whdr *Subscriber wire protocol v2* and a tolerant
 * parser. Every server/client frame in the appendix is represented here.
 *
 * Forward-compatibility rules (conformance item 10) are enforced by
 * {@link parseServerFrame}: unknown `type` tags and unknown object fields are
 * ignored. `seq`/`ts_ms`/`through_seq` etc. are `u64` on the wire; JSON carries
 * them as numbers, which is exact up to 2^53 — far beyond realistic webhook
 * sequence counts.
 */

/** Reason carried by a `closing` frame. */
export type ClosingReason = "shutdown" | "revoked";

/** First frame after a successful auth upgrade; echoes the token's label. */
export interface WelcomeFrame {
  type: "welcome";
  name: string;
}

/** Acknowledges a `subscribe`/`unsubscribe` (`op` is the client op name). */
export interface OkFrame {
  type: "ok";
  op: string;
}

/**
 * A non-fatal op failure. `op:"subscribe"` for a bad pattern; `op:"replay"`
 * when durable delivery is disabled. Only `op` is contractual — never match on
 * `msg` text.
 */
export interface ErrorFrame {
  type: "error";
  op: string;
  msg: string;
}

/**
 * A delivered event. `id` is stable across live delivery and every replay —
 * **dedup by `id`**. `seq` is the global monotonic cursor key; `ts_ms` is the
 * server wall-clock at fan-out and is informational (order by `seq`, not
 * `ts_ms`).
 */
export interface EventFrame {
  type: "event";
  id: string;
  seq: number;
  ts_ms: number;
  channel: string;
  payload_b64: string;
}

/** Sent after a replay window is fully delivered; live frames follow. */
export interface ReplayedFrame {
  type: "replayed";
  through_seq: number;
}

/**
 * Explicit data-loss signal: the requested `after_seq` (`from_seq`) predates
 * retention. Events in `(from_seq, earliest_seq)` are permanently pruned;
 * replay resumes from `earliest_seq` (which itself IS delivered).
 */
export interface ReplayGapFrame {
  type: "replay_gap";
  from_seq: number;
  earliest_seq: number;
}

/**
 * The outbound queue evicted `dropped` events for this connection. Recover by
 * reconnecting and replaying from the cursor.
 */
export interface LaggedFrame {
  type: "lagged";
  dropped: number;
}

/** Reply to a client `{"type":"ping"}`. */
export interface PongFrame {
  type: "pong";
}

/** The server is closing this connection. */
export interface ClosingFrame {
  type: "closing";
  reason: ClosingReason;
}

/** Any server → client frame. */
export type ServerFrame =
  | WelcomeFrame
  | OkFrame
  | ErrorFrame
  | EventFrame
  | ReplayedFrame
  | ReplayGapFrame
  | LaggedFrame
  | PongFrame
  | ClosingFrame;

/** Optional resume cursor on `subscribe`. */
export interface ReplayRequest {
  after_seq: number;
}

/** Add channel patterns; with `replay`, stream stored events first. */
export interface SubscribeFrame {
  type: "subscribe";
  patterns: string[];
  replay?: ReplayRequest;
}

/** Remove channel patterns. */
export interface UnsubscribeFrame {
  type: "unsubscribe";
  patterns: string[];
}

/** Application-level liveness; server replies `pong`. */
export interface PingFrame {
  type: "ping";
}

/** Any client → server frame. */
export type ClientFrame = SubscribeFrame | UnsubscribeFrame | PingFrame;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

/**
 * Parse one JSON text frame into a typed {@link ServerFrame}, or `null` if the
 * frame is unrecognised.
 *
 * Returns `null` for invalid JSON, non-object payloads, unknown `type` tags
 * (conformance item 10), and known frames missing a required field. Unknown
 * *extra* fields on a known frame are tolerated (ignored) — the caller simply
 * reads the next frame when this returns `null`.
 */
export function parseServerFrame(text: string): ServerFrame | null {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return null;
  }
  if (!isRecord(value)) return null;

  switch (value["type"]) {
    case "welcome": {
      const name = asString(value["name"]);
      return name === undefined ? null : { type: "welcome", name };
    }
    case "ok": {
      const op = asString(value["op"]);
      return op === undefined ? null : { type: "ok", op };
    }
    case "error": {
      const op = asString(value["op"]);
      const msg = asString(value["msg"]);
      return op === undefined || msg === undefined ? null : { type: "error", op, msg };
    }
    case "event": {
      const id = asString(value["id"]);
      const seq = asNumber(value["seq"]);
      const ts_ms = asNumber(value["ts_ms"]);
      const channel = asString(value["channel"]);
      const payload_b64 = asString(value["payload_b64"]);
      if (
        id === undefined ||
        seq === undefined ||
        ts_ms === undefined ||
        channel === undefined ||
        payload_b64 === undefined
      ) {
        return null;
      }
      return { type: "event", id, seq, ts_ms, channel, payload_b64 };
    }
    case "replayed": {
      const through_seq = asNumber(value["through_seq"]);
      return through_seq === undefined ? null : { type: "replayed", through_seq };
    }
    case "replay_gap": {
      const from_seq = asNumber(value["from_seq"]);
      const earliest_seq = asNumber(value["earliest_seq"]);
      return from_seq === undefined || earliest_seq === undefined
        ? null
        : { type: "replay_gap", from_seq, earliest_seq };
    }
    case "lagged": {
      const dropped = asNumber(value["dropped"]);
      return dropped === undefined ? null : { type: "lagged", dropped };
    }
    case "pong":
      return { type: "pong" };
    case "closing": {
      const reason = value["reason"];
      return reason === "shutdown" || reason === "revoked"
        ? { type: "closing", reason }
        : null;
    }
    default:
      // Unknown frame type: ignore (forward compatibility).
      return null;
  }
}

/** Build a `subscribe` frame, attaching a resume cursor when `afterSeq` is set. */
export function subscribeFrame(
  patterns: string[],
  afterSeq: number | undefined,
): SubscribeFrame {
  return afterSeq === undefined
    ? { type: "subscribe", patterns: [...patterns] }
    : { type: "subscribe", patterns: [...patterns], replay: { after_seq: afterSeq } };
}
