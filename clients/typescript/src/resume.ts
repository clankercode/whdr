/**
 * Cursor + dedup state implementing the appendix's at-least-once guard
 * (conformance items 4 & 5).
 *
 * An event is processed at most once, and the cursor advances only *after* a
 * successful handle. `seq` is a **global** monotonic counter, so gaps in the
 * `seq` values a single connection observes are normal (they belong to other
 * subscribers' patterns) — never infer loss from a gap.
 */
export class ResumeState {
  #cursor: number;
  readonly #seen = new Set<string>();
  readonly #order: string[] = [];
  readonly #capacity: number;

  /**
   * @param cursor    resume cursor (highest seq processed; 0 = from retention start)
   * @param capacity  recent-`id` window size for boundary dedup
   */
  constructor(cursor: number, capacity: number) {
    this.#cursor = cursor;
    this.#capacity = Math.max(1, capacity);
  }

  /** Highest `seq` processed so far — the value to send as `replay.after_seq`. */
  cursor(): number {
    return this.#cursor;
  }

  /**
   * Whether an event with this `id`/`seq` should be handed to the handler.
   * Skips duplicates around the replay/live boundary: a `seq` at or below the
   * cursor, or an `id` already processed within the recent window.
   */
  shouldProcess(id: string, seq: number): boolean {
    return seq > this.#cursor && !this.#seen.has(id);
  }

  /**
   * Record a successfully-handled event: remember its `id` (evicting the
   * oldest beyond `capacity`) and advance the cursor.
   */
  record(id: string, seq: number): void {
    if (!this.#seen.has(id)) {
      this.#seen.add(id);
      this.#order.push(id);
      if (this.#order.length > this.#capacity) {
        const old = this.#order.shift();
        if (old !== undefined) this.#seen.delete(old);
      }
    }
    if (seq > this.#cursor) this.#cursor = seq;
  }
}
