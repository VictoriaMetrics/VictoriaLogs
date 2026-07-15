import { describe, expect, it } from "vitest";
import { generateIntervalsMs } from "./intervals";
import { secondsToNanoseconds } from "./time";

function expectStrictlyIncreasing(values: number[]) {
  for (let i = 1; i < values.length; i++) {
    expect(values[i]).toBeGreaterThan(values[i - 1]);
  }
}

function expectAllFinitePositive(values: number[]) {
  for (const value of values) {
    expect(Number.isFinite(value)).toBe(true);
    expect(value).toBeGreaterThan(0);
  }
}

describe("generateIntervalsMs", () => {
  it("returns [] for zero-length ranges", () => {
    expect(generateIntervalsMs({
      start: 0n,
      end: 0n,
    })).toEqual([]);
  });

  it("returns 7 unique ascending intervals for a valid range", () => {
    const out = generateIntervalsMs({
      start: secondsToNanoseconds(0),
      end: secondsToNanoseconds(10 * 24 * 60 * 60), // 10 days
    });

    expect(out).toHaveLength(7);
    expect(new Set(out).size).toBe(7);
    expectStrictlyIncreasing(out);
    expectAllFinitePositive(out);
  });

  it("allows sub-500ms intervals for ranges below 1 minute", () => {
    const out = generateIntervalsMs({
      start: secondsToNanoseconds(0),
      end: secondsToNanoseconds(10), // 10 seconds
    });

    expect(out).toHaveLength(7);
    expectStrictlyIncreasing(out);
    expect(out.some((v) => v < 500)).toBe(true);
  });

  it("does not use intervals below 500ms for ranges >= 1 minute", () => {
    const out = generateIntervalsMs({
      start: secondsToNanoseconds(0),
      end: secondsToNanoseconds(60), // exactly 1 minute
    });

    expect(out).toHaveLength(7);
    expectStrictlyIncreasing(out);
    expect(Math.min(...out)).toBeGreaterThanOrEqual(500);
  });

  it("keeps the exact regression output for a 5-minute range", () => {
    const start = secondsToNanoseconds(1771418375.671);
    const end = secondsToNanoseconds(1771418675.671); // +300s

    expect(generateIntervalsMs({ start, end })).toEqual([
      500,
      1000,
      2000,
      5000,
      10000,
      15000,
      30000,
    ]);
  });
});
