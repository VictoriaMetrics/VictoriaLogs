// useProcessStatsQueryRange.test.ts
import { describe, it, expect, beforeEach, vi } from "vitest";
import useProcessStatsQueryRange, { ResponseMatrix } from "./useProcessStatsQueryRange";
import { MetricResult } from "../../../api/types";

// Helper: build N series, each with the same values
const makeSeries = (count: number, value: number): MetricResult[] =>
  Array.from({ length: count }, (_, i) => ({
    group: 0,
    metric: { name: `series_${i}` },
    values: [
      [1697040000, value.toString()],
      [1697040060, value.toString()],
    ],
  }));

describe("useProcessStatsQueryRange", () => {
  let setLogHits: ReturnType<typeof vi.fn>;
  let setError: ReturnType<typeof vi.fn>;
  let processStats: ReturnType<typeof useProcessStatsQueryRange>;

  beforeEach(() => {
    setLogHits = vi.fn();
    setError = vi.fn();
    processStats = useProcessStatsQueryRange({ setLogHits, setError });
  });

  it("returns error and empty result when result is missing", () => {
    // Intentionally pass malformed data (without `data.result`)
    const data = { status: "error" };

    const res = processStats(data, 5);

    expect(setError).toHaveBeenCalledWith("Error: No 'result' field in response");
    expect(setLogHits).toHaveBeenCalledWith([]);
    expect(res).toEqual([]);
  });

  it("returns empty result when series is empty", () => {
    const data: ResponseMatrix = {
      status: "success",
      data: {
        result: [],
        resultType: "matrix",
      },
    };

    const res = processStats(data, 5);

    expect(setError).not.toHaveBeenCalled();
    expect(setLogHits).toHaveBeenCalledWith([]);
    expect(res).toEqual([]);
  });

  it("returns all series without Other when series count is less than limit", () => {
    const series = makeSeries(3, 10);
    const data: ResponseMatrix = {
      status: "success",
      data: {
        result: series,
        resultType: "matrix",
      },
    };

    const res = processStats(data, 5);

    expect(res.length).toBe(3);
    expect(res.some((s) => s._isOther)).toBe(false);
    expect(setLogHits).toHaveBeenCalledWith(res);
  });

  it("returns all series without Other when series count equals limit", () => {
    const series = makeSeries(5, 7);
    const data: ResponseMatrix = {
      status: "success",
      data: {
        result: series,
        resultType: "matrix",
      },
    };

    const res = processStats(data, 5);

    expect(res.length).toBe(5);
    expect(res.some((s) => s._isOther)).toBe(false);
  });

  it("adds Other at the beginning and returns limit top series when series count is greater than limit", () => {
    // 3 "big" series + 3 "small" series
    const big = makeSeries(3, 100);  // large total
    const small = makeSeries(3, 1);  // small total
    const data: ResponseMatrix = {
      status: "success",
      data: {
        result: [...big, ...small],
        resultType: "matrix",
      },
    };

    const limit = 3;
    const res = processStats(data, limit);

    // 1 Other + limit top series = limit + 1
    expect(res.length).toBe(limit + 1);

    // Other should be the first series
    expect(res[0]._isOther).toBe(true);

    // The rest should be normal series
    for (let i = 1; i < res.length; i++) {
      expect(res[i]._isOther).toBe(false);
    }

    // total of Other = sum of totals of the tail series
    const other = res[0];
    const value = 1;
    const otherTotalExpected = small.length * small[0].values.length * value; // 3 series * 2 points * 1
    expect(other.total).toBe(otherTotalExpected);
  });

  it("returns single series without Other when there is only one series", () => {
    const series = makeSeries(1, 42);
    const data: ResponseMatrix = {
      status: "success",
      data: {
        result: series,
        resultType: "matrix",
      },
    };

    const res = processStats(data, 5);

    expect(res.length).toBe(1);
    expect(res[0]._isOther).toBe(false);
  });

  it("aggregates everything into a single Other series when limit = 0", () => {
    const series = makeSeries(3, 10);
    const data: ResponseMatrix = {
      status: "success",
      data: {
        result: series,
        resultType: "matrix",
      },
    };

    const res = processStats(data, 0);

    expect(res.length).toBe(1);
    expect(res[0]._isOther).toBe(true);

    const expectedTotal = 3 * 2 * 10; // 3 series * 2 points * value=10
    expect(res[0].total).toBe(expectedTotal);
  });

  it("keeps timestamps and values arrays in top series in sync", () => {
    const series = makeSeries(4, 5);
    const data: ResponseMatrix = {
      status: "success",
      data: {
        result: series,
        resultType: "matrix",
      },
    };

    const res = processStats(data, 2); // 1 Other + 2 top series

    const normalSeries = res.filter((s) => !s._isOther);
    for (const s of normalSeries) {
      expect(s.timestamps.length).toBe(s.values.length);
    }
  });
});
