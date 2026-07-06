/**
 * `whdr-sub-kit` (TypeScript) — client library for the whdr subscriber plane,
 * implementing the *Subscriber wire protocol v2* (durable delivery / replay).
 *
 * Mirrors the behaviour of the reference Rust crate `whdr-sub-kit`. See
 * `docs/SPEC.md` §9/§9.4 and `docs/SUBSCRIBERS.md`.
 *
 * @example
 * ```ts
 * import { WhdrSubscriber } from "whdr-sub-kit";
 *
 * const sub = new WhdrSubscriber({
 *   url: "ws://127.0.0.1:8788/subscribe",
 *   token: process.env.WHDR_TOKEN!,
 *   patterns: ["github.>"],
 *   cursor: 0, // replay from the start of retention
 * });
 *
 * // Runs forever, reconnecting with backoff; rejects only on a fatal error.
 * await sub.run({
 *   async onEvent(event) {
 *     console.log(`seq=${event.seq} ${event.channel} ${event.payload().length} bytes`);
 *   },
 *   onReplayGap(from, earliest) {
 *     console.error(`replay_gap: events (${from}, ${earliest}) were pruned`);
 *   },
 * });
 * ```
 */

export {
  WhdrSubscriber,
  decodePayload,
  type DeliveredEvent,
  type Handler,
  type WhdrSubscriberOptions,
  type RunOptions,
  type StreamOptions,
} from "./subscriber.js";

export { Connection, buildHeaders } from "./connection.js";

export {
  MemoryCursorStore,
  cursorStoreFromCallbacks,
  type CursorStore,
} from "./cursor.js";

export { ResumeState } from "./resume.js";

export {
  Backoff,
  DEFAULT_BACKOFF,
  baseDelayMs,
  applyJitter,
  type BackoffPolicy,
} from "./backoff.js";

export {
  parseServerFrame,
  subscribeFrame,
  type ServerFrame,
  type ClientFrame,
  type ClosingReason,
  type WelcomeFrame,
  type OkFrame,
  type ErrorFrame,
  type EventFrame,
  type ReplayedFrame,
  type ReplayGapFrame,
  type LaggedFrame,
  type PongFrame,
  type ClosingFrame,
  type ReplayRequest,
  type SubscribeFrame,
  type UnsubscribeFrame,
  type PingFrame,
} from "./frames.js";

export {
  WhdrError,
  AuthError,
  HttpError,
  RevokedError,
  HandlerError,
  CursorStoreError,
  ConnectionClosedError,
  RequestError,
} from "./errors.js";
