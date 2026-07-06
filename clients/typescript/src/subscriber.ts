/**
 * {@link WhdrSubscriber} — the batteries-included reconnect-and-resume loop.
 *
 * Implements the appendix §7 algorithm end-to-end: auth → welcome → subscribe
 * with `replay.after_seq = cursor` → dedup by `id` and `seq <= cursor` →
 * advance/persist the cursor only after a successful handle → recover from
 * `lagged` / dropped sockets by reconnecting from the cursor → surface
 * `replay_gap` → treat `revoked` as fatal and `shutdown` as a backoff
 * reconnect → exponential backoff with jitter → ignore unknown frames.
 */
import type { ClientOptions } from "ws";

import { Backoff, DEFAULT_BACKOFF, type BackoffPolicy } from "./backoff.js";
import { Connection } from "./connection.js";
import {
  MemoryCursorStore,
  type CursorStore,
} from "./cursor.js";
import {
  CursorStoreError,
  HandlerError,
  RevokedError,
  WhdrError,
} from "./errors.js";
import type { EventFrame } from "./frames.js";
import { ResumeState } from "./resume.js";

/** Decode a standard-base64 payload to raw bytes. */
export function decodePayload(payloadB64: string): Uint8Array {
  return new Uint8Array(Buffer.from(payloadB64, "base64"));
}

/**
 * A delivered event handed to a {@link Handler}. The kit has already
 * de-duplicated by `id`/`seq`, so a handler sees each event at most once. Dedup
 * key is `id`; order by `seq`, not `ts_ms`.
 */
export interface DeliveredEvent {
  readonly id: string;
  readonly seq: number;
  readonly ts_ms: number;
  readonly channel: string;
  readonly payload_b64: string;
  /** Decode `payload_b64` to raw bytes. */
  payload(): Uint8Array;
}

function makeDeliveredEvent(frame: EventFrame): DeliveredEvent {
  return {
    id: frame.id,
    seq: frame.seq,
    ts_ms: frame.ts_ms,
    channel: frame.channel,
    payload_b64: frame.payload_b64,
    payload: () => decodePayload(frame.payload_b64),
  };
}

/**
 * Callbacks for the {@link WhdrSubscriber.run} loop. Only {@link Handler.onEvent}
 * is required. **Throwing from any hook is fatal**: `run` stops and rethrows a
 * {@link HandlerError}. The cursor advances (and persists) only *after*
 * `onEvent` resolves — at-least-once delivery.
 */
export interface Handler {
  /** Handle a delivered event. On success the cursor advances to `event.seq`. */
  onEvent(event: DeliveredEvent): void | Promise<void>;
  /** A replay window finished at `throughSeq`; live frames follow. */
  onReplayed?(throughSeq: number): void | Promise<void>;
  /**
   * Explicit data-loss signal: events in `(fromSeq, earliestSeq)` were pruned
   * before this subscriber resumed. Reconcile out-of-band if you must.
   */
  onReplayGap?(fromSeq: number, earliestSeq: number): void | Promise<void>;
  /** The server evicted `dropped` events; the kit reconnects and replays. */
  onLagged?(dropped: number): void | Promise<void>;
  /** A `replay` request was refused (durability disabled); live still works. */
  onReplayUnavailable?(msg: string): void | Promise<void>;
}

/** Construction options for a {@link WhdrSubscriber}. */
export interface WhdrSubscriberOptions {
  /** `/subscribe` endpoint, e.g. `ws://127.0.0.1:8788/subscribe`. */
  url: string;
  /** `tok_…` subscriber token minted by the operator. */
  token: string;
  /** NATS-style channel patterns, e.g. `["github.>"]`. */
  patterns: string[];
  /** Initial resume cursor (default 0 = replay from retention start). Ignored when `cursorStore` is set. */
  cursor?: number;
  /** Cursor-persistence hook for at-least-once across restarts. */
  cursorStore?: CursorStore;
  /** Backoff policy overrides (merged over the defaults). */
  backoff?: Partial<BackoffPolicy>;
  /** Recent-`id` dedup window size (default 8192). */
  dedupCapacity?: number;
  /** Extra `ws` client options (e.g. TLS). */
  wsOptions?: ClientOptions;
  /** Jitter source, injectable for tests (default `Math.random`). */
  rand?: () => number;
  /** Sleep implementation, injectable for tests (default abortable `setTimeout`). */
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
}

/** Options for a single {@link WhdrSubscriber.run} invocation. */
export interface RunOptions {
  /** Abort to stop the loop cleanly (closes the socket, no reconnect). */
  signal?: AbortSignal;
}

/** Options for {@link WhdrSubscriber.stream}: an abort signal plus signal hooks. */
export interface StreamOptions extends RunOptions {
  onReplayed?(throughSeq: number): void | Promise<void>;
  onReplayGap?(fromSeq: number, earliestSeq: number): void | Promise<void>;
  onLagged?(dropped: number): void | Promise<void>;
  onReplayUnavailable?(msg: string): void | Promise<void>;
}

function defaultSleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = (): void => {
      clearTimeout(timer);
      resolve();
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

export class WhdrSubscriber {
  readonly #url: string;
  readonly #token: string;
  readonly #patterns: string[];
  readonly #cursorStore: CursorStore;
  readonly #backoffPolicy: BackoffPolicy;
  readonly #dedupCapacity: number;
  readonly #wsOptions: ClientOptions;
  readonly #rand: () => number;
  readonly #sleep: (ms: number, signal?: AbortSignal) => Promise<void>;

  constructor(options: WhdrSubscriberOptions) {
    this.#url = options.url;
    this.#token = options.token;
    this.#patterns = [...options.patterns];
    this.#cursorStore =
      options.cursorStore ?? new MemoryCursorStore(options.cursor ?? 0);
    this.#backoffPolicy = { ...DEFAULT_BACKOFF, ...options.backoff };
    this.#dedupCapacity = Math.max(1, options.dedupCapacity ?? 8192);
    this.#wsOptions = options.wsOptions ?? {};
    this.#rand = options.rand ?? Math.random;
    this.#sleep = options.sleep ?? defaultSleep;
  }

  /**
   * Connect, authenticate, and subscribe once with the configured patterns and
   * cursor. Returns a ready {@link Connection} for bespoke loops; most callers
   * want {@link WhdrSubscriber.run}.
   */
  async connect(): Promise<Connection> {
    const cursor = await this.#cursorStore.load();
    const conn = await Connection.connect(this.#url, this.#token, this.#wsOptions);
    await conn.subscribe(this.#patterns, cursor);
    return conn;
  }

  /**
   * Run the full reconnect-and-resume loop, driving `handler`. Loops forever,
   * reconnecting with backoff after a transient failure. Returns when `signal`
   * aborts; rejects only on a **fatal** error (auth/revoked/handler/cursor).
   */
  async run(handler: Handler, options: RunOptions = {}): Promise<void> {
    const signal = options.signal;
    const initial = await this.#cursorStore.load();
    const resume = new ResumeState(initial, this.#dedupCapacity);
    const backoff = new Backoff(this.#backoffPolicy, this.#rand);

    while (!signal?.aborted) {
      try {
        await this.#runSession(handler, resume, backoff, signal);
      } catch (err) {
        if (err instanceof WhdrError && err.fatal) throw err;
        if (signal?.aborted) return;
        // Transient: fall through to backoff + reconnect.
      }
      if (signal?.aborted) return;
      await this.#sleep(backoff.nextDelayMs(), signal);
    }
  }

  /**
   * Async-iterator event API. Yields de-duplicated {@link DeliveredEvent}s while
   * the reconnect-and-resume loop runs in the background. Non-event signals are
   * delivered via the optional hooks in `options`. Call `return()` on the
   * iterator (or abort `options.signal`) to stop.
   */
  stream(options: StreamOptions = {}): AsyncIterableIterator<DeliveredEvent> {
    const controller = new AbortController();
    if (options.signal) {
      if (options.signal.aborted) controller.abort();
      else options.signal.addEventListener("abort", () => controller.abort(), { once: true });
    }

    const queue = new EventQueue();
    const handler: Handler = {
      onEvent: (event) => queue.push(event),
      ...(options.onReplayed ? { onReplayed: options.onReplayed } : {}),
      ...(options.onReplayGap ? { onReplayGap: options.onReplayGap } : {}),
      ...(options.onLagged ? { onLagged: options.onLagged } : {}),
      ...(options.onReplayUnavailable
        ? { onReplayUnavailable: options.onReplayUnavailable }
        : {}),
    };

    this.run(handler, { signal: controller.signal }).then(
      () => queue.finish(),
      (err) => queue.fail(err),
    );

    const iterator: AsyncIterableIterator<DeliveredEvent> = {
      next: () => queue.next(),
      return: async () => {
        controller.abort();
        queue.finish();
        return { done: true, value: undefined };
      },
      [Symbol.asyncIterator]() {
        return iterator;
      },
    };
    return iterator;
  }

  async #runSession(
    handler: Handler,
    resume: ResumeState,
    backoff: Backoff,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    const conn = await Connection.connect(this.#url, this.#token, this.#wsOptions);
    const onAbort = (): void => conn.close();
    signal?.addEventListener("abort", onAbort, { once: true });
    try {
      // Connected: reset backoff so a later drop reconnects fast.
      backoff.reset();
      await conn.subscribe(this.#patterns, resume.cursor());

      for (;;) {
        const frame = await conn.recv();
        switch (frame.type) {
          case "event": {
            if (resume.shouldProcess(frame.id, frame.seq)) {
              const event = makeDeliveredEvent(frame);
              await callHook(() => handler.onEvent(event));
              resume.record(frame.id, frame.seq);
              try {
                await this.#cursorStore.save(resume.cursor());
              } catch (err) {
                throw new CursorStoreError(err);
              }
            }
            break;
          }
          case "replayed":
            await callHook(() => handler.onReplayed?.(frame.through_seq));
            break;
          case "replay_gap":
            await callHook(() =>
              handler.onReplayGap?.(frame.from_seq, frame.earliest_seq),
            );
            break;
          case "lagged":
            await callHook(() => handler.onLagged?.(frame.dropped));
            // Recover by reconnecting and replaying from the cursor.
            return;
          case "error":
            if (frame.op === "replay") {
              await callHook(() => handler.onReplayUnavailable?.(frame.msg));
            }
            // Other errors (e.g. bad pattern) are non-fatal; keep the connection.
            break;
          case "closing":
            if (frame.reason === "revoked") throw new RevokedError();
            // shutdown: reconnect with backoff.
            return;
          case "welcome":
          case "ok":
          case "pong":
            // Nothing to do.
            break;
        }
      }
    } finally {
      signal?.removeEventListener("abort", onAbort);
      conn.close();
    }
  }
}

/** Await an optional hook, wrapping any throw as a fatal {@link HandlerError}. */
async function callHook(fn: () => void | Promise<void>): Promise<void> {
  try {
    await fn();
  } catch (err) {
    if (err instanceof WhdrError) throw err;
    throw new HandlerError(err);
  }
}

/** A single-consumer async queue bridging the callback loop to an iterator. */
class EventQueue {
  readonly #items: DeliveredEvent[] = [];
  #resolve: ((result: IteratorResult<DeliveredEvent>) => void) | null = null;
  #reject: ((err: unknown) => void) | null = null;
  #done = false;
  #error: unknown = null;

  push(event: DeliveredEvent): void {
    if (this.#resolve) {
      const resolve = this.#resolve;
      this.#resolve = null;
      this.#reject = null;
      resolve({ done: false, value: event });
    } else {
      this.#items.push(event);
    }
  }

  finish(): void {
    this.#done = true;
    if (this.#resolve) {
      const resolve = this.#resolve;
      this.#resolve = null;
      this.#reject = null;
      resolve({ done: true, value: undefined });
    }
  }

  fail(err: unknown): void {
    this.#error = err;
    if (this.#reject) {
      const reject = this.#reject;
      this.#resolve = null;
      this.#reject = null;
      reject(err);
    }
  }

  next(): Promise<IteratorResult<DeliveredEvent>> {
    const item = this.#items.shift();
    if (item !== undefined) return Promise.resolve({ done: false, value: item });
    if (this.#error !== null) return Promise.reject(this.#error);
    if (this.#done) return Promise.resolve({ done: true, value: undefined });
    return new Promise((resolve, reject) => {
      this.#resolve = resolve;
      this.#reject = reject;
    });
  }
}
