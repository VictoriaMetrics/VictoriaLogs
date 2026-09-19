import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook } from "@testing-library/preact";

import { useTimePeriod } from "./useTimePeriod";

// -------------------- mocks --------------------

// mock react-router-dom useSearchParams WITHOUT bringing the real router,
// so the test can count how many times the URL is rewritten
let initialParams = new URLSearchParams();
let setSearchParamsCalls = 0;
let lastNavigateOpts: { replace?: boolean } | undefined;

vi.mock("react-router-dom", async () => {
  const { useState } = await import("preact/compat");

  const useSearchParams = () => {
    const [params, setParams] = useState<URLSearchParams>(initialParams);

    const setSearchParams = (updater: unknown, navigateOpts?: { replace?: boolean }) => {
      setSearchParamsCalls++;
      lastNavigateOpts = navigateOpts;
      setParams((prev: URLSearchParams) => {
        const next = typeof updater === "function" ? updater(prev) : updater;
        return next instanceof URLSearchParams ? next : new URLSearchParams(next);
      });
    };

    return [params, setSearchParams] as const;
  };

  return { useSearchParams };
});

// executeQueryTrigger is bumped by the auto-refresh timer
let executeQueryTrigger = 0;

vi.mock("../../../state/query/QueryStateContext", async () => {
  return {
    useQueryState: () => ({ executeQueryTrigger }),
  };
});

const setUrlParams = (entries: Record<string, string>) => {
  initialParams = new URLSearchParams(entries);
};

describe("useTimePeriod", () => {
  beforeEach(() => {
    setSearchParamsCalls = 0;
    lastNavigateOpts = undefined;
    executeQueryTrigger = 0;
    setUrlParams({});
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-06T12:00:00.000Z"));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("refreshPeriod", () => {
    it("does not rewrite the URL when a relative range is already described in it", () => {
      setUrlParams({
        "g0.relative_time": "last_5_minutes",
        "g0.range_input": "5m",
        "g0.end_input": "2026-08-06T12:00:00.000Z",
      });

      const { result } = renderHook(() => useTimePeriod());
      setSearchParamsCalls = 0;

      expect(result.current.refreshPeriod()).toBe(true);
      expect(setSearchParamsCalls).toBe(0);
    });

    it("does not rewrite the URL on repeated refreshes", () => {
      setUrlParams({
        "g0.relative_time": "last_1_hour",
        "g0.range_input": "1h",
      });

      const { result } = renderHook(() => useTimePeriod());
      setSearchParamsCalls = 0;

      result.current.refreshPeriod();
      result.current.refreshPeriod();
      result.current.refreshPeriod();

      expect(setSearchParamsCalls).toBe(0);
    });

    it("seeds the time params once, replacing rather than pushing, when the URL does not describe a range yet", () => {
      setUrlParams({});

      const { result } = renderHook(() => useTimePeriod());
      setSearchParamsCalls = 0;
      lastNavigateOpts = undefined;

      expect(result.current.refreshPeriod()).toBe(true);
      expect(setSearchParamsCalls).toBe(1);
      expect(lastNavigateOpts).toEqual({ replace: true });
    });

    it("seeds the time params for a bare relative_time=none without an end_input", () => {
      // not reachable through the UI, which always writes all three keys, but a
      // hand-edited URL can produce it
      setUrlParams({ "g0.relative_time": "none" });

      const { result } = renderHook(() => useTimePeriod());
      setSearchParamsCalls = 0;
      lastNavigateOpts = undefined;

      expect(result.current.refreshPeriod()).toBe(true);
      expect(setSearchParamsCalls).toBe(1);
      expect(lastNavigateOpts).toEqual({ replace: true });
    });

    it("returns false and leaves the URL alone for an absolute range", () => {
      setUrlParams({
        "g0.relative_time": "none",
        "g0.range_input": "5m",
        "g0.end_input": "2026-08-06T12:00:00.000Z",
      });

      const { result } = renderHook(() => useTimePeriod());
      setSearchParamsCalls = 0;

      expect(result.current.refreshPeriod()).toBe(false);
      expect(setSearchParamsCalls).toBe(0);
    });
  });

  describe("period", () => {
    it("advances a relative range when the query trigger fires", () => {
      setUrlParams({
        "g0.relative_time": "last_5_minutes",
        "g0.range_input": "5m",
      });

      const { result, rerender } = renderHook(() => useTimePeriod());
      const before = result.current.period.end;

      vi.setSystemTime(new Date("2026-08-06T12:00:30.000Z"));
      executeQueryTrigger++;
      rerender();

      expect(result.current.period.end).toBeGreaterThan(before);
      expect(setSearchParamsCalls).toBe(0);
    });

    it("keeps an absolute range pinned when the query trigger fires", () => {
      setUrlParams({
        "g0.relative_time": "none",
        "g0.range_input": "5m",
        "g0.end_input": "2026-08-06T12:00:00.000Z",
      });

      const { result, rerender } = renderHook(() => useTimePeriod());
      const before = result.current.period.end;

      vi.setSystemTime(new Date("2026-08-06T12:00:30.000Z"));
      executeQueryTrigger++;
      rerender();

      expect(result.current.period.end).toBe(before);
    });
  });
});
