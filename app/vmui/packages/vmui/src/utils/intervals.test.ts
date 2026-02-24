import { describe, expect, it } from "vitest";
import { generateIntervalsMs } from "./intervals";

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
  it("returns [] for zero-length or invalid ranges", () => {
    expect(generateIntervalsMs(0, 0)).toEqual([]);
    expect(generateIntervalsMs(Number.NaN, 10)).toEqual([]);
    expect(generateIntervalsMs(10, Number.NaN)).toEqual([]);
    expect(generateIntervalsMs(Number.POSITIVE_INFINITY, 10)).toEqual([]);
    expect(generateIntervalsMs(10, Number.NEGATIVE_INFINITY)).toEqual([]);
  });

  it("does not depend on start/end order", () => {
    expect(generateIntervalsMs(100, 10_000)).toEqual(
      generateIntervalsMs(10_000, 100),
    );
  });

  it("returns 7 unique ascending intervals for a valid range", () => {
    const out = generateIntervalsMs(0, 10 * 24 * 60 * 60); // 10 days

    expect(out).toHaveLength(7);
    expect(new Set(out).size).toBe(7);
    expectStrictlyIncreasing(out);
    expectAllFinitePositive(out);
  });

  it("allows sub-500ms intervals for ranges below 1 minute", () => {
    const out = generateIntervalsMs(0, 10); // 10 seconds

    expect(out).toHaveLength(7);
    expectStrictlyIncreasing(out);
    expect(out.some((v) => v < 500)).toBe(true);
  });

  it("does not use intervals below 500ms for ranges >= 1 minute", () => {
    const out = generateIntervalsMs(0, 60); // exactly 1 minute

    expect(out).toHaveLength(7);
    expectStrictlyIncreasing(out);
    expect(Math.min(...out)).toBeGreaterThanOrEqual(500);
  });

  it("keeps the exact regression output for a 5-minute range", () => {
    const start = 1771418375.671;
    const end = 1771418675.671; // +300s

    expect(generateIntervalsMs(start, end)).toEqual([
      500, 1000, 2000, 5000, 10000, 15000, 30000,
    ]);
  });
});
