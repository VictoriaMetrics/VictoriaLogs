import { act, cleanup, renderHook } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useQueryPageController } from "./index";
import type { FetchHitsResult } from "../useFetchHits";

const mocks = vi.hoisted(() => ({
  runHits: vi.fn(),
  runLogs: vi.fn(),
  resetHits: vi.fn(),
  resetLogs: vi.fn(),
  abortHits: vi.fn(),
  abortLogs: vi.fn(),
  fetchQueryTime: vi.fn(),
  abortTime: vi.fn(),
  dispatch: vi.fn(),
}));

const initialBase = {
  query: "*",
  period: { start: 1_800_000_000_000_000_000n, end: 1_800_086_400_000_000_000n },
  extraParams: new URLSearchParams(),
  setPeriod: vi.fn(),
};
const initialHits = {
  isChartHidden: false,
  step: "6h",
  groupFieldHits: "none",
  topHits: 5,
  graphQueryMode: "hits",
};
const logsTriggers = { isLogsHidden: false, beforeFetch: undefined };
let baseTriggers = initialBase;
let hitsTriggers = initialHits;
let finishHits: ((result: FetchHitsResult) => void) | undefined;

vi.mock("../useQueryPageTriggers/", () => ({
  useBaseTriggers: () => baseTriggers,
  useHitsTriggers: () => hitsTriggers,
  useLogsTriggers: () => logsTriggers,
}));
vi.mock("./useHitsController", () => ({
  useHitsController: () => ({ runHits: mocks.runHits, abort: mocks.abortHits, resetHits: mocks.resetHits }),
}));
vi.mock("./useLogsController", () => ({
  useLogsController: () => ({ runLogs: mocks.runLogs, abort: mocks.abortLogs, resetLogs: mocks.resetLogs }),
}));
vi.mock("../useFetchQueryTime", () => ({
  useFetchQueryTime: () => ({ fetchQueryTime: mocks.fetchQueryTime, abort: mocks.abortTime }),
}));
vi.mock("../../../../state/query/QueryStateContext", () => ({
  useQueryDispatch: () => mocks.dispatch,
}));
vi.mock("../../../../components/QueryHistory/utils", () => ({
  addQueryToHistoryStorage: vi.fn(),
}));

const flushDebounce = async () => {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(300);
  });
};

const completeHits = async (result: FetchHitsResult = "success") => {
  expect(finishHits).toBeDefined();
  await act(async () => {
    finishHits!(result);
    finishHits = undefined;
  });
};

describe("useQueryPageController: logs timeout fallback", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.resetAllMocks();
    baseTriggers = initialBase;
    hitsTriggers = initialHits;
    finishHits = undefined;
    mocks.fetchQueryTime.mockResolvedValue(undefined);
    mocks.runLogs.mockResolvedValue({ status: "timeout" });
    mocks.abortHits.mockImplementation(() => {
      finishHits?.("aborted");
      finishHits = undefined;
    });
    // Match useFetchHits: starting another hits request cancels the previous one.
    mocks.runHits.mockImplementation(() => {
      mocks.abortHits();
      return new Promise<FetchHitsResult>(resolve => {
        finishHits = resolve;
      });
    });
  });

  afterEach(() => {
    cleanup();
    finishHits?.("aborted");
    finishHits = undefined;
    vi.clearAllTimers();
    vi.useRealTimers();
  });

  it("retries timed-out logs after incremental hits finish", async () => {
    renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();

    expect(mocks.runHits).toHaveBeenCalledTimes(1);
    expect(mocks.runHits).toHaveBeenCalledWith(expect.objectContaining({ isIterative: true }));
    expect(mocks.resetLogs).toHaveBeenCalledTimes(1);
    expect(mocks.resetHits).toHaveBeenCalledTimes(1);
    expect(mocks.resetLogs.mock.invocationCallOrder[0]).toBeLessThan(mocks.runHits.mock.invocationCallOrder[0]);
    expect(mocks.resetHits.mock.invocationCallOrder[0]).toBeLessThan(mocks.runHits.mock.invocationCallOrder[0]);
    expect(mocks.runLogs).toHaveBeenCalledTimes(1);

    await completeHits();

    expect(mocks.runLogs).toHaveBeenCalledTimes(2);
    expect(mocks.runLogs).toHaveBeenLastCalledWith(expect.objectContaining({ query: "*", shouldUseLogsTimeout: false }));
  });

  it("loads pending logs after hits fail", async () => {
    const { result } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();

    expect(result.current.logsRequestState.isPending).toBe(true);
    expect(mocks.runLogs).toHaveBeenCalledTimes(1);

    await completeHits("error");

    expect(mocks.runLogs).toHaveBeenCalledTimes(2);
    expect(mocks.runLogs).toHaveBeenCalledWith(expect.objectContaining({
      query: baseTriggers.query,
      period: baseTriggers.period,
    }));
    expect(result.current.logsRequestState.isPending).toBe(false);
  });

  it("clears pending logs without loading them after cancellation", async () => {
    const { result } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    expect(result.current.logsRequestState.isPending).toBe(true);
    mocks.abortHits.mockClear();

    await act(async () => {
      result.current.cancelAll();
    });

    expect(mocks.abortHits).toHaveBeenCalledTimes(1);
    expect(mocks.runLogs).toHaveBeenCalledTimes(1);
    expect(result.current.logsRequestState.isPending).toBe(false);
  });

  it("preserves loaded logs when only the chart step changes", async () => {
    mocks.runLogs.mockResolvedValue({ status: "success" });
    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await completeHits();

    mocks.resetLogs.mockClear();
    mocks.resetHits.mockClear();
    mocks.runLogs.mockClear();

    hitsTriggers = { ...initialHits, step: "1h" };
    rerender();
    await flushDebounce();

    expect(mocks.resetHits).toHaveBeenCalledTimes(1);
    expect(mocks.resetLogs).not.toHaveBeenCalled();
    expect(mocks.runLogs).not.toHaveBeenCalled();

    await completeHits();

    expect(mocks.resetLogs).not.toHaveBeenCalled();
    expect(mocks.runLogs).not.toHaveBeenCalled();
  });

  it("preserves the new query when its chart step changes during fallback", async () => {
    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await completeHits();
    mocks.runLogs.mockClear();

    baseTriggers = { ...initialBase, query: "error" };
    rerender();
    await flushDebounce();

    hitsTriggers = { ...initialHits, step: "1h" };
    rerender();
    await flushDebounce();
    expect(mocks.runHits).toHaveBeenLastCalledWith(expect.objectContaining({
      query: "error", step: "1h", isIterative: true,
    }));

    await completeHits();
    expect(mocks.runLogs).toHaveBeenLastCalledWith(expect.objectContaining({
      query: "error", shouldUseLogsTimeout: false,
    }));
  });

  it("loads successful logs before ordinary hits without retrying logs", async () => {
    mocks.runLogs.mockResolvedValue({ status: "success" });
    renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();

    expect(mocks.runLogs.mock.invocationCallOrder[0]).toBeLessThan(mocks.runHits.mock.invocationCallOrder[0]);
    expect(mocks.runHits).toHaveBeenCalledWith(expect.objectContaining({ isIterative: false }));
    await completeHits();
    expect(mocks.runLogs).toHaveBeenCalledTimes(1);
  });

  it("resolves the time filter after chart changes and ignores stale responses", async () => {
    type TimeResult = typeof initialBase.period & { hasTimeFilter: boolean };
    const finishTime: Array<(value: TimeResult | undefined) => void> = [];
    mocks.fetchQueryTime.mockImplementation(() => new Promise<TimeResult | undefined>(resolve => {
      finishTime.push(resolve);
    }));
    mocks.runLogs.mockResolvedValue({ status: "success" });
    baseTriggers = { ...initialBase, query: "_time:7d" };

    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();

    hitsTriggers = { ...initialHits, step: "1h" };
    rerender();
    await flushDebounce();
    expect(mocks.fetchQueryTime).toHaveBeenCalledTimes(2);
    expect(mocks.runLogs).not.toHaveBeenCalled();
    expect(mocks.runHits).not.toHaveBeenCalled();

    await act(async () => { finishTime[0](undefined); });
    expect(mocks.dispatch).not.toHaveBeenCalled();

    hitsTriggers = { ...initialHits, step: "2h" };
    rerender();
    await flushDebounce();
    expect(mocks.fetchQueryTime).toHaveBeenCalledTimes(3);
    expect(mocks.runLogs).not.toHaveBeenCalled();

    const resolvedPeriod = {
      start: initialBase.period.start - 86_400_000_000_000n,
      end: initialBase.period.end,
    };
    await act(async () => { finishTime[2]({ ...resolvedPeriod, hasTimeFilter: true }); });
    expect(baseTriggers.setPeriod).toHaveBeenCalledTimes(1);
    expect(mocks.runLogs).not.toHaveBeenCalled();

    baseTriggers = { ...baseTriggers, period: resolvedPeriod };
    rerender();
    await flushDebounce();
    expect(mocks.fetchQueryTime).toHaveBeenCalledTimes(3);
    expect(mocks.runLogs).toHaveBeenCalledWith(expect.objectContaining({ period: resolvedPeriod }));
    expect(mocks.runHits).toHaveBeenCalledWith(expect.objectContaining({ period: resolvedPeriod, step: "2h" }));

    await act(async () => { finishTime[1](undefined); });
    expect(baseTriggers.setPeriod).toHaveBeenCalledTimes(1);
    expect(mocks.runLogs).toHaveBeenCalledTimes(1);
    await completeHits();

    hitsTriggers = { ...initialHits, step: "3h" };
    rerender();
    await flushDebounce();
    expect(mocks.fetchQueryTime).toHaveBeenCalledTimes(3);
    expect(mocks.runLogs).toHaveBeenCalledTimes(1);
    await completeHits();
  });

  it("loads hits without retrying logs when loading logs is declined", async () => {
    mocks.runLogs.mockResolvedValue({ status: "aborted" });
    const { result } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();

    expect(mocks.runHits).toHaveBeenCalledTimes(1);
    expect(mocks.runHits).toHaveBeenCalledWith(expect.objectContaining({ isIterative: false }));
    expect(result.current.logsRequestState.isPending).toBe(false);

    await completeHits();

    expect(mocks.runLogs).toHaveBeenCalledTimes(1);
    expect(result.current.logsRequestState.isPending).toBe(false);
  });

  it.each(["aborted", "timeout"])("does not continue a cancelled logs request returning %s", async (status) => {
    let finishLogs!: (result: { status: string }) => void;
    mocks.runLogs.mockImplementation(() => new Promise(resolve => { finishLogs = resolve; }));
    const { result } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await act(async () => {
      result.current.cancelAll();
      finishLogs({ status });
    });
    expect(mocks.runHits).not.toHaveBeenCalled();
    expect(mocks.runLogs).toHaveBeenCalledTimes(1);
  });
});
