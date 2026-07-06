/**
 * Cursor persistence hook.
 *
 * Implement {@link CursorStore} to make at-least-once delivery survive process
 * restarts: `load` is called once at {@link WhdrSubscriber.run} start, and
 * `save` after each successfully-handled event. For not-missing-while-briefly-
 * disconnected only, the default {@link MemoryCursorStore} is enough.
 */
export interface CursorStore {
  /** Load the last persisted cursor (0 = replay from the start of retention). */
  load(): Promise<number> | number;
  /** Persist a cursor value. Called after each successfully-handled event. */
  save(cursor: number): Promise<void> | void;
}

/** In-memory cursor store seeded from an initial value; not durable. */
export class MemoryCursorStore implements CursorStore {
  #cursor: number;

  constructor(initial = 0) {
    this.#cursor = initial;
  }

  load(): number {
    return this.#cursor;
  }

  save(cursor: number): void {
    this.#cursor = cursor;
  }

  /** Current cursor value (for tests / inspection). */
  get(): number {
    return this.#cursor;
  }
}

/**
 * Adapt plain load/save callbacks into a {@link CursorStore}. Convenient when a
 * caller just wants to persist the cursor with two functions.
 */
export function cursorStoreFromCallbacks(callbacks: {
  load: () => Promise<number> | number;
  save: (cursor: number) => Promise<void> | void;
}): CursorStore {
  return { load: callbacks.load, save: callbacks.save };
}
