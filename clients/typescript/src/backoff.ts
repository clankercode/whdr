/**
 * Exponential backoff with jitter for reconnect scheduling.
 *
 * Delays follow `initial * multiplier^attempt`, capped at `max`, then scaled by
 * a random factor in `[1 - jitter, 1 + jitter)`. A fresh {@link Backoff} resets
 * to `attempt = 0`; the run loop resets it after every successful connection so
 * a long-lived connection that later drops reconnects fast.
 */
export interface BackoffPolicy {
  /** Delay (ms) before the first reconnect attempt. */
  initialMs: number;
  /** Upper bound (ms) on the pre-jitter delay. */
  maxMs: number;
  /** Growth factor applied per attempt. */
  multiplier: number;
  /** Jitter fraction in `[0, 1)`. `0.2` = ±20%. */
  jitter: number;
}

export const DEFAULT_BACKOFF: BackoffPolicy = {
  initialMs: 500,
  maxMs: 30_000,
  multiplier: 2.0,
  jitter: 0.2,
};

/** The deterministic (pre-jitter) base delay in ms for an attempt number. */
export function baseDelayMs(policy: BackoffPolicy, attempt: number): number {
  const factor = Math.pow(policy.multiplier, attempt);
  return Math.min(policy.initialMs * factor, policy.maxMs);
}

/**
 * Pure jitter application, factored out for testability. `rand01` is a sample
 * in `[0, 1)`; the result stays within `[base*(1-jitter), base*(1+jitter))`.
 */
export function applyJitter(baseMs: number, jitter: number, rand01: number): number {
  if (jitter <= 0) return baseMs;
  const factor = 1 - jitter + rand01 * 2 * jitter;
  return baseMs * factor;
}

/** Running backoff state for a {@link BackoffPolicy}. */
export class Backoff {
  #attempt = 0;

  constructor(
    private readonly policy: BackoffPolicy,
    private readonly rand: () => number = Math.random,
  ) {}

  /** Reset to the initial delay (call after a successful connection). */
  reset(): void {
    this.#attempt = 0;
  }

  /** Compute the next delay (ms, with jitter) and advance the attempt counter. */
  nextDelayMs(): number {
    const base = baseDelayMs(this.policy, this.#attempt);
    this.#attempt += 1;
    return applyJitter(base, this.policy.jitter, this.rand());
  }
}
