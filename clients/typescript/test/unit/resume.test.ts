import { describe, expect, test } from "vitest";

import { ResumeState } from "../../src/resume.js";

const id = (n: number): string => `00000000-0000-0000-0000-${String(n).padStart(12, "0")}`;

describe("ResumeState", () => {
  test("skips seq at or below the cursor (conformance item 4)", () => {
    const state = new ResumeState(0, 16);
    expect(state.shouldProcess(id(1), 1)).toBe(true);
    state.record(id(1), 1);
    expect(state.cursor()).toBe(1);
    // A replayed duplicate at seq 1 is now below the cursor.
    expect(state.shouldProcess(id(1), 1)).toBe(false);
    // A brand-new lower seq is still guarded.
    expect(state.shouldProcess(id(99), 1)).toBe(false);
    // The next higher seq proceeds.
    expect(state.shouldProcess(id(2), 2)).toBe(true);
  });

  test("dedups by id across the replay/live boundary (conformance item 4)", () => {
    const state = new ResumeState(5, 16);
    expect(state.shouldProcess(id(7), 6)).toBe(true);
    state.record(id(7), 6);
    // Same id delivered twice (once replay, once live): processed once.
    expect(state.shouldProcess(id(7), 6)).toBe(false);
    // Even at a higher seq label, a seen id is skipped.
    expect(state.shouldProcess(id(7), 8)).toBe(false);
  });

  test("cursor advances only via record (conformance item 5)", () => {
    const state = new ResumeState(10, 16);
    expect(state.shouldProcess(id(1), 11)).toBe(true);
    expect(state.cursor()).toBe(10); // asking does not move it
    state.record(id(1), 11);
    expect(state.cursor()).toBe(11);
  });

  test("bounded seen set evicts oldest but seq guard still holds", () => {
    const state = new ResumeState(0, 2);
    state.record(id(1), 1);
    state.record(id(2), 2);
    state.record(id(3), 3); // evicts id(1) from the recent set
    // id(1) evicted from the id set, but seq 1 is below the cursor (3).
    expect(state.shouldProcess(id(1), 1)).toBe(false);
    // id(2) still remembered by id.
    expect(state.shouldProcess(id(2), 2)).toBe(false);
  });

  test("global seq gaps are not treated as loss (item 5 note)", () => {
    // A connection may observe seq 3 then 9 (4..8 went to other patterns).
    const state = new ResumeState(0, 16);
    state.record(id(3), 3);
    expect(state.cursor()).toBe(3);
    expect(state.shouldProcess(id(9), 9)).toBe(true);
    state.record(id(9), 9);
    expect(state.cursor()).toBe(9);
  });
});
