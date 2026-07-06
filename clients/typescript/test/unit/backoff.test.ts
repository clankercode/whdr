import { describe, expect, test } from "vitest";

import { applyJitter, Backoff, baseDelayMs, DEFAULT_BACKOFF } from "../../src/backoff.js";

describe("baseDelayMs", () => {
  test("grows exponentially and caps at max", () => {
    const policy = { initialMs: 500, maxMs: 8000, multiplier: 2.0, jitter: 0 };
    expect(baseDelayMs(policy, 0)).toBe(500);
    expect(baseDelayMs(policy, 1)).toBe(1000);
    expect(baseDelayMs(policy, 2)).toBe(2000);
    expect(baseDelayMs(policy, 3)).toBe(4000);
    expect(baseDelayMs(policy, 4)).toBe(8000);
    expect(baseDelayMs(policy, 5)).toBe(8000); // capped
  });
});

describe("applyJitter", () => {
  test("stays within [1-j, 1+j) of base", () => {
    const base = 1000;
    expect(applyJitter(base, 0.2, 0)).toBeCloseTo(800); // lower extreme
    expect(applyJitter(base, 0.2, 0.9999)).toBeGreaterThanOrEqual(base);
    expect(applyJitter(base, 0.2, 0.9999)).toBeLessThan(1200);
    expect(applyJitter(base, 0.2, 0.5)).toBeCloseTo(1000); // midpoint
  });

  test("zero jitter is exact", () => {
    expect(applyJitter(1000, 0, 0.5)).toBe(1000);
  });
});

describe("Backoff", () => {
  test("advances then reset returns to the initial delay", () => {
    // Fixed rand => deterministic; default policy has jitter, so pin rand=0.5
    // to land on the exact base delays.
    const b = new Backoff({ ...DEFAULT_BACKOFF, jitter: 0 }, () => 0.5);
    const first = b.nextDelayMs();
    expect(first).toBe(500);
    expect(b.nextDelayMs()).toBe(1000);
    expect(b.nextDelayMs()).toBe(2000);
    b.reset();
    expect(b.nextDelayMs()).toBe(first);
  });

  test("jitter keeps successive delays bounded", () => {
    const b = new Backoff(DEFAULT_BACKOFF, () => 0); // lower extreme
    // attempt 0 base=500 => 0.8*500 = 400
    expect(b.nextDelayMs()).toBeCloseTo(400);
  });
});
