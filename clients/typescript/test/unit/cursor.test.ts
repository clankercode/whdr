import { describe, expect, test } from "vitest";

import { cursorStoreFromCallbacks, MemoryCursorStore } from "../../src/cursor.js";

describe("MemoryCursorStore", () => {
  test("round-trips the seeded value", async () => {
    const store = new MemoryCursorStore(42);
    expect(await store.load()).toBe(42);
    await store.save(100);
    expect(await store.load()).toBe(100);
    expect(store.get()).toBe(100);
  });

  test("defaults to 0", async () => {
    expect(await new MemoryCursorStore().load()).toBe(0);
  });
});

describe("cursorStoreFromCallbacks", () => {
  test("delegates load/save to the provided callbacks", async () => {
    let saved = 7;
    const store = cursorStoreFromCallbacks({
      load: () => saved,
      save: (c) => {
        saved = c;
      },
    });
    expect(await store.load()).toBe(7);
    await store.save(11);
    expect(saved).toBe(11);
  });
});
