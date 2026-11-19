import { act, cleanup, renderHook } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useQueryPageController } from "./useQueryPageController";
import type { FetchLogsParams } from "../useFetchLogs";
import type { FetchHitsResult } from "../useFetchHits";

const mocks = vi.hoisted(() => ({
  fetchHits: vi.fn(),
  fetchLogs: vi.fn(),
  resetHits: vi.fn(),
  resetLogs: vi.fn(),
  abortHits: vi.fn(),
  abortLogs: vi.fn(),
  fetchQueryTime: vi.fn(),
  abortTime: vi.fn(),
  dispatch: vi.fn(),
  beforeFetch: vi.fn(),
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
const logsTriggers = { isLogsHidden: false, beforeFetch: mocks.beforeFetch };
let limit = 1000;
let timeoutMs = 3000;
let baseTriggers = initialBase;
let hitsTriggers = initialHits;
let finishTimeoutLogs: (() => void) | undefined;
const pendingLogs = ({ onStart, limit, beforeFetch }: FetchLogsParams) => {
  onStart?.();
  if (!beforeFetch) return Promise.resolve({ status: "success", limit });
  return new Promise(resolve => {
    finishTimeoutLogs = () => { resolve({ status: "aborted", limit }); finishTimeoutLogs = undefined; };
  });
};
let finishHits: ((result: FetchHitsResult) => void) | undefined;

vi.mock("../useQueryPageTriggers/", () => ({
  useBaseTriggers: () => baseTriggers,
  useHitsTriggers: () => hitsTriggers,
  useLogsTriggers: () => logsTriggers,
}));
vi.mock("../useFetchHits", () => ({
  useFetchHits: () => ({ fetchHits: mocks.fetchHits, abort: mocks.abortHits, resetHits: mocks.resetHits }),
}));
vi.mock("../../HitsPanel/hooks/useIncrementalTimeout", () => ({
  useIncrementalTimeout: () => ({ incrementalTimeoutMs: timeoutMs }),
}));
vi.mock("../useFetchLogs", () => ({
  useFetchLogs: () => ({ fetchLogs: mocks.fetchLogs, abort: mocks.abortLogs, resetLogs: mocks.resetLogs }),
}));
vi.mock("../../../../components/Configurators/LogsLimitController/hooks/useLimitController", () => ({
  useLimitController: () => ({ limit }),
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

const elapseIncrementalTimeout = async () => {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(timeoutMs);
  });
};

const completeHits = async (result: FetchHitsResult = "success") => {
  expect(finishHits).toBeDefined();
  await act(async () => {
    finishHits!(result);
    finishHits = undefined;
    await vi.advanceTimersByTimeAsync(0);
  });
};

describe("useQueryPageController: logs timeout fallback", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.resetAllMocks();
    limit = 1000;
    timeoutMs = 3000;
    baseTriggers = initialBase;
    hitsTriggers = initialHits;
    finishHits = undefined;
    mocks.fetchQueryTime.mockResolvedValue(undefined);
    finishTimeoutLogs = undefined;
    mocks.fetchLogs.mockImplementation(pendingLogs);
    mocks.abortLogs.mockImplementation(() => finishTimeoutLogs?.());
    mocks.abortHits.mockImplementation(() => {
      finishHits?.("aborted");
      finishHits = undefined;
    });
    // Match useFetchHits: starting another hits request cancels the previous one.
    mocks.fetchHits.mockImplementation(() => {
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

  it("switches timed-out hits without resetting or reloading successful logs", async () => {
    mocks.fetchLogs.mockResolvedValue({ status: "success" });

    renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    expect(mocks.fetchHits).toHaveBeenNthCalledWith(1, expect.objectContaining({
      isIterative: false,
    }));
    await act(async () => { await vi.advanceTimersByTimeAsync(3000); });
    expect(mocks.fetchHits).toHaveBeenNthCalledWith(2, expect.objectContaining({ isIterative: true }));
    await completeHits();
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(mocks.resetLogs).toHaveBeenCalledTimes(1);
  });

  it("waits for logs before loading hits with a timeout when the chart is shown", async () => {
    let finishLogs!: () => void;
    mocks.fetchLogs.mockImplementation(({ onStart }: FetchLogsParams) => {
      onStart?.();
      return new Promise(resolve => { finishLogs = () => resolve({ status: "success" }); });
    });
    hitsTriggers = { ...initialHits, isChartHidden: true };
    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));

    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    const abortCount = mocks.abortLogs.mock.calls.length;

    // With the chart hidden, logs can run longer than the incremental timeout.
    await act(async () => { await vi.advanceTimersByTimeAsync(timeoutMs); });
    expect(mocks.abortLogs).toHaveBeenCalledTimes(abortCount);
    expect(mocks.fetchHits).not.toHaveBeenCalled();

    hitsTriggers = { ...initialHits, isChartHidden: false };
    rerender();
    // Showing the chart must not impose a timeout on the existing logs request.
    await act(async () => { await vi.advanceTimersByTimeAsync(300 + timeoutMs); });

    expect(mocks.abortLogs).toHaveBeenCalledTimes(abortCount);
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(mocks.fetchHits).not.toHaveBeenCalled();

    await act(async () => {
      finishLogs();
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(mocks.fetchHits).toHaveBeenCalledTimes(1);
    expect(mocks.fetchHits).toHaveBeenNthCalledWith(1, expect.objectContaining({ isIterative: false }));
    const hitsAbortCount = mocks.abortHits.mock.calls.length;

    await act(async () => { await vi.advanceTimersByTimeAsync(timeoutMs - 1); });
    expect(mocks.fetchHits).toHaveBeenCalledTimes(1);
    expect(mocks.abortHits).toHaveBeenCalledTimes(hitsAbortCount);
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(mocks.abortHits.mock.calls.length).toBeGreaterThan(hitsAbortCount);
    expect(mocks.fetchHits).toHaveBeenCalledTimes(2);
    expect(mocks.fetchHits).toHaveBeenNthCalledWith(2, expect.objectContaining({ isIterative: true }));

    await completeHits();
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(mocks.resetLogs).toHaveBeenCalledTimes(1);
    expect(mocks.abortLogs).toHaveBeenCalledTimes(abortCount);
  });

  it("keeps the original logs timeout across multiple hits step changes", async () => {
    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    const abortCount = mocks.abortLogs.mock.calls.length;
    for (const step of ["1h", "2h"]) {
      hitsTriggers = { ...initialHits, step };
      rerender();
      await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    }
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(mocks.abortLogs).toHaveBeenCalledTimes(abortCount);
    await act(async () => { await vi.advanceTimersByTimeAsync(2399); });
    expect(mocks.fetchHits).not.toHaveBeenCalled();
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(mocks.fetchHits).toHaveBeenCalledWith(expect.objectContaining({ step: "2h", isIterative: true }));
    await completeHits();
  });

  it.each([false, true])("uses latest hits settings when logs finish before debounce (hidden=%s)", async hidden => {
    let finishLogs!: () => void;
    mocks.fetchLogs.mockImplementation(({ onStart }: FetchLogsParams) => {
      onStart?.();
      return new Promise(resolve => { finishLogs = () => resolve({ status: "success" }); });
    });
    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    hitsTriggers = { ...initialHits, step: "1h", isChartHidden: hidden };
    rerender();
    await act(async () => { finishLogs(); });
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(mocks.fetchHits).toHaveBeenCalledTimes(hidden ? 0 : 1);
    if (!hidden) {
      expect(mocks.fetchHits).toHaveBeenCalledWith(expect.objectContaining({ step: "1h", isIterative: false }));
      await completeHits();
    }
  });

  it.each(["cancel", "new query"])("ignores an aborted hits request after %s", async action => {
    mocks.fetchLogs.mockResolvedValue({ status: "success" });
    let finish!: (value: FetchHitsResult) => void;
    mocks.fetchHits.mockImplementationOnce(() => new Promise<FetchHitsResult>(resolve => { finish = resolve; }));
    const { result, rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    if (action === "cancel") {
      act(() => result.current.cancelAll());
    } else {
      baseTriggers = { ...initialBase, query: "error" };
      rerender();
      await flushDebounce();
    }
    await act(async () => { finish("aborted"); });
    expect(mocks.fetchHits).toHaveBeenCalledTimes(action === "cancel" ? 1 : 2);
    expect(mocks.fetchHits.mock.calls.every(([params]) => !params.isIterative)).toBe(true);
    if (action !== "cancel") await completeHits();
  });

  it.each(["off", "stats", "once"])("disables the hits timeout for %s", async mode => {
    mocks.fetchLogs.mockResolvedValue({ status: "success" });

    if (mode === "off") timeoutMs = 0;
    if (mode === "stats") hitsTriggers = { ...initialHits, graphQueryMode: "stats" };
    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    if (mode === "once") {
      await completeHits();
      hitsTriggers = Object.assign({}, initialHits, { executeHitsOnceTrigger: 1 });
      rerender();
      await flushDebounce();
    }
    const callCount = mocks.fetchHits.mock.calls.length;
    await act(async () => { await vi.advanceTimersByTimeAsync(6000); });
    expect(mocks.fetchHits).toHaveBeenCalledTimes(callCount);
    expect(mocks.fetchHits).toHaveBeenLastCalledWith(expect.objectContaining({ isIterative: false }));
    await completeHits();
  });

  it.each([false, true])("preserves the original logs limit on retry (step changed=%s)", async changeStep => {
    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await elapseIncrementalTimeout();
    expect(mocks.fetchLogs).toHaveBeenCalledWith(expect.objectContaining({ limit: 1000 }));

    limit = 2000;
    if (changeStep) hitsTriggers = { ...initialHits, step: "1h" };
    rerender();
    await flushDebounce();
    await completeHits();

    expect(mocks.fetchLogs).toHaveBeenCalledTimes(2);
    expect(mocks.fetchLogs).toHaveBeenLastCalledWith(expect.objectContaining({
      limit: 1000,
      beforeFetch: undefined,
    }));
  });

  it("retries with the limit confirmed by the guard rather than the requested limit", async () => {
    mocks.fetchLogs.mockImplementation(params => pendingLogs({ ...params, limit: 500 }));
    renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await elapseIncrementalTimeout();
    expect(mocks.fetchLogs).toHaveBeenCalledWith(expect.objectContaining({ limit: 1000 }));
    await completeHits();
    expect(mocks.fetchLogs).toHaveBeenLastCalledWith(expect.objectContaining({
      limit: 500,
      beforeFetch: undefined,
    }));
  });

  it("retries timed-out logs after incremental hits finish", async () => {
    renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await elapseIncrementalTimeout();

    expect(mocks.fetchHits).toHaveBeenCalledTimes(1);
    expect(mocks.fetchHits).toHaveBeenCalledWith(expect.objectContaining({ isIterative: true }));
    expect(mocks.resetLogs).toHaveBeenCalledTimes(1);
    expect(mocks.resetHits).toHaveBeenCalledTimes(1);
    expect(mocks.resetLogs.mock.invocationCallOrder[0]).toBeLessThan(mocks.fetchHits.mock.invocationCallOrder[0]);
    expect(mocks.resetHits.mock.invocationCallOrder[0]).toBeLessThan(mocks.fetchHits.mock.invocationCallOrder[0]);
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(mocks.fetchLogs).toHaveBeenCalledWith(expect.objectContaining({ beforeFetch: mocks.beforeFetch }));

    await completeHits();

    expect(mocks.fetchLogs).toHaveBeenCalledTimes(2);
    expect(mocks.fetchLogs).toHaveBeenLastCalledWith(expect.objectContaining({
      query: "*",
      beforeFetch: undefined,
    }));
  });

  it("loads pending logs after hits fail", async () => {
    const { result } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await elapseIncrementalTimeout();

    expect(result.current.logsRequestState.isPending).toBe(true);
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);

    await completeHits("error");

    expect(mocks.fetchLogs).toHaveBeenCalledTimes(2);
    expect(mocks.fetchLogs).toHaveBeenCalledWith(expect.objectContaining({
      query: baseTriggers.query,
      period: baseTriggers.period,
    }));
    expect(result.current.logsRequestState.isPending).toBe(false);
  });

  it("clears pending logs without loading them after cancellation", async () => {
    const { result } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await elapseIncrementalTimeout();
    expect(result.current.logsRequestState.isPending).toBe(true);
    mocks.abortHits.mockClear();

    await act(async () => {
      result.current.cancelAll();
    });

    expect(mocks.abortHits).toHaveBeenCalledTimes(1);
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(result.current.logsRequestState.isPending).toBe(false);
  });

  it("preserves loaded logs when only the chart step changes", async () => {
    mocks.fetchLogs.mockResolvedValue({ status: "success" });
    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await completeHits();

    mocks.resetLogs.mockClear();
    mocks.resetHits.mockClear();
    mocks.fetchLogs.mockClear();

    hitsTriggers = { ...initialHits, step: "1h" };
    rerender();
    await flushDebounce();

    expect(mocks.resetHits).toHaveBeenCalledTimes(1);
    expect(mocks.resetLogs).not.toHaveBeenCalled();
    expect(mocks.fetchLogs).not.toHaveBeenCalled();

    await completeHits();

    expect(mocks.resetLogs).not.toHaveBeenCalled();
    expect(mocks.fetchLogs).not.toHaveBeenCalled();
  });

  it("preserves the new query when its chart step changes during fallback", async () => {
    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await elapseIncrementalTimeout();
    await completeHits();
    mocks.fetchLogs.mockClear();

    baseTriggers = { ...initialBase, query: "error" };
    rerender();
    await flushDebounce();
    await elapseIncrementalTimeout();
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(mocks.fetchLogs).toHaveBeenLastCalledWith(expect.objectContaining({
      query: "error", onStart: expect.any(Function),
    }));

    hitsTriggers = { ...initialHits, step: "1h" };
    rerender();
    await flushDebounce();
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(mocks.fetchHits).toHaveBeenLastCalledWith(expect.objectContaining({
      query: "error", step: "1h", isIterative: true,
    }));

    await completeHits();
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(2);
    expect(mocks.fetchLogs).toHaveBeenLastCalledWith(expect.objectContaining({
      query: "error",
    }));
  });

  it("keeps waiting for hits when their step changes after a logs timeout", async () => {
    mocks.fetchLogs.mockImplementationOnce(pendingLogs).mockResolvedValue({ status: "success" });
    const { result, rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await elapseIncrementalTimeout();

    for (const step of ["1h", "2h"]) {
      hitsTriggers = { ...initialHits, step };
      rerender();
      await flushDebounce();
      expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
      expect(mocks.fetchHits).toHaveBeenLastCalledWith(expect.objectContaining({ step, isIterative: true }));
      expect(result.current.logsRequestState.isPending).toBe(true);
    }

    await completeHits();
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(2);
    expect(mocks.fetchLogs).toHaveBeenLastCalledWith(expect.objectContaining({
      beforeFetch: undefined,
    }));
    expect(result.current.logsRequestState.isPending).toBe(false);

    hitsTriggers = { ...initialHits, step: "3h" };
    rerender();
    await flushDebounce();
    expect(mocks.fetchHits).toHaveBeenLastCalledWith(expect.objectContaining({ isIterative: false }));
    await completeHits();
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(2);
  });

  it.each(["success", "error"] as const)("waits for updated hits before retrying logs when old hits finish with %s before debounce", async status => {
    const { result, rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await elapseIncrementalTimeout();

    hitsTriggers = { ...initialHits, step: "1h" };
    rerender();
    await completeHits(status);

    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(mocks.fetchHits).toHaveBeenCalledTimes(1);
    expect(result.current.logsRequestState.isPending).toBe(true);

    await flushDebounce();

    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(mocks.fetchHits).toHaveBeenCalledTimes(2);
    expect(mocks.fetchHits).toHaveBeenLastCalledWith(expect.objectContaining({
      step: "1h", isIterative: true,
    }));
    expect(result.current.logsRequestState.isPending).toBe(true);

    await completeHits();

    expect(mocks.fetchLogs).toHaveBeenCalledTimes(2);
    expect(mocks.fetchLogs).toHaveBeenLastCalledWith(expect.objectContaining({
      limit: 1000, beforeFetch: undefined,
    }));
    expect(result.current.logsRequestState.isPending).toBe(false);
  });

  it.each([0, 300])("preserves retrying logs and applies latest hits step (debounce elapsed=%s)", async debounceMs => {
    let finishRetry!: () => void;
    mocks.fetchLogs.mockImplementationOnce(pendingLogs).mockImplementationOnce(() => new Promise(resolve => {
      finishRetry = () => resolve({ status: "success", limit: 1000 });
    }));
    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await elapseIncrementalTimeout();
    await completeHits();
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(2);
    const abortCount = mocks.abortLogs.mock.calls.length;

    for (const step of ["1h", "2h"]) {
      hitsTriggers = { ...initialHits, step };
      rerender();
      await act(async () => { await vi.advanceTimersByTimeAsync(debounceMs); });
    }
    expect(mocks.abortLogs).toHaveBeenCalledTimes(abortCount);
    expect(mocks.fetchHits).toHaveBeenCalledTimes(1);
    await act(async () => { finishRetry(); });
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    expect(mocks.fetchHits).toHaveBeenCalledTimes(2);
    expect(mocks.fetchHits).toHaveBeenLastCalledWith(expect.objectContaining({ step: "2h", isIterative: false }));

    await act(async () => { await vi.advanceTimersByTimeAsync(3000); });
    expect(mocks.fetchHits).toHaveBeenLastCalledWith(expect.objectContaining({ step: "2h", isIterative: true }));
    await completeHits();
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(2);
  });

  it.each(["cancel", "query", "hide"])("does not start stale hits after logs retry when %s", async action => {
    let finishRetry!: () => void;
    mocks.fetchLogs.mockImplementationOnce(pendingLogs).mockImplementationOnce(() => new Promise(resolve => {
      finishRetry = () => resolve({ status: "success", limit: 1000 });
    })).mockResolvedValue({ status: "success" });
    const { result, rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await elapseIncrementalTimeout();
    await completeHits();
    hitsTriggers = { ...initialHits, step: "1h", isChartHidden: action === "hide" };
    rerender();
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    if (action === "cancel") act(() => result.current.cancelAll());
    if (action === "query") {
      baseTriggers = { ...initialBase, query: "error" };
      rerender();
      await flushDebounce();
    }
    const hitsCount = mocks.fetchHits.mock.calls.length;
    await act(async () => { finishRetry(); });
    expect(mocks.fetchHits).toHaveBeenCalledTimes(hitsCount);
    if (action === "query") await completeHits();
  });

  it("retries logs without a timeout when the chart is hidden during fallback", async () => {
    let finishRetry!: () => void;
    mocks.fetchLogs.mockImplementationOnce(pendingLogs).mockImplementationOnce(({ onStart }: FetchLogsParams) => {
      onStart?.();
      return new Promise(resolve => {
        finishRetry = () => resolve({ status: "success", limit: 1000 });
      });
    });
    const { result, rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await elapseIncrementalTimeout();

    // useFetchHits aborts immediately when the chart is hidden, before the debounced sync.
    await completeHits("aborted");
    hitsTriggers = { ...initialHits, isChartHidden: true };
    rerender();
    await flushDebounce();

    expect(mocks.fetchHits).toHaveBeenCalledTimes(1);
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(2);
    expect(mocks.fetchLogs).toHaveBeenLastCalledWith(expect.objectContaining({
      beforeFetch: undefined,
    }));
    expect(result.current.logsRequestState.isPending).toBe(false);
    const abortCount = mocks.abortLogs.mock.calls.length;

    await act(async () => { await vi.advanceTimersByTimeAsync(timeoutMs * 2); });

    expect(mocks.abortLogs).toHaveBeenCalledTimes(abortCount);
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(2);
    expect(mocks.fetchHits).toHaveBeenCalledTimes(1);

    await act(async () => {
      finishRetry();
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(2);
    expect(mocks.fetchHits).toHaveBeenCalledTimes(1);
  });

  it("starts with logs again when the query changes during fallback", async () => {
    mocks.fetchLogs.mockImplementationOnce(pendingLogs).mockResolvedValue({ status: "success" });
    const { result, rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await elapseIncrementalTimeout();
    expect(result.current.logsRequestState.isPending).toBe(true);

    baseTriggers = { ...initialBase, query: "error" };
    rerender();
    await flushDebounce();

    expect(mocks.fetchLogs).toHaveBeenCalledTimes(2);
    expect(mocks.fetchLogs).toHaveBeenLastCalledWith(expect.objectContaining({
      query: "error", onStart: expect.any(Function), beforeFetch: mocks.beforeFetch,
    }));
    expect(mocks.fetchHits).toHaveBeenLastCalledWith(expect.objectContaining({ query: "error", isIterative: false }));
    await completeHits();
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(2);
    expect(result.current.logsRequestState.isPending).toBe(false);
  });

  it("loads successful logs before ordinary hits without retrying logs", async () => {
    mocks.fetchLogs.mockResolvedValue({ status: "success" });
    renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();

    expect(mocks.fetchLogs.mock.invocationCallOrder[0]).toBeLessThan(mocks.fetchHits.mock.invocationCallOrder[0]);
    expect(mocks.fetchHits).toHaveBeenCalledWith(expect.objectContaining({ isIterative: false }));
    await completeHits();
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
  });

  it.each([false, true])("keeps logs scheduled when the step changes during time sync (same period=%s)", async samePeriod => {
    type TimeResult = typeof initialBase.period & { hasTimeFilter: boolean };
    const finishTime: Array<(value: TimeResult | undefined) => void> = [];
    mocks.fetchQueryTime.mockImplementation(() => new Promise<TimeResult | undefined>(resolve => {
      finishTime.push(resolve);
    }));
    mocks.fetchLogs.mockResolvedValue({ status: "success" });
    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();

    hitsTriggers = { ...initialHits, step: "1h" };
    rerender();
    await flushDebounce();
    expect(mocks.fetchQueryTime).toHaveBeenCalledTimes(2);
    expect(mocks.fetchLogs).not.toHaveBeenCalled();

    await act(async () => {
      finishTime[1](samePeriod ? { ...initialBase.period, hasTimeFilter: true } : undefined);
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(baseTriggers.setPeriod).not.toHaveBeenCalled();
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(mocks.fetchLogs).toHaveBeenCalledWith(expect.objectContaining({ period: initialBase.period }));
    expect(mocks.fetchHits).toHaveBeenCalledWith(expect.objectContaining({ step: "1h", isIterative: false }));
    expect(mocks.fetchLogs.mock.invocationCallOrder[0]).toBeLessThan(mocks.fetchHits.mock.invocationCallOrder[0]);

    await act(async () => { finishTime[0](undefined); });
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(mocks.fetchHits).toHaveBeenCalledTimes(1);
    await completeHits();
  });

  it("resolves the time filter after chart changes and ignores stale responses", async () => {
    type TimeResult = typeof initialBase.period & { hasTimeFilter: boolean };
    const finishTime: Array<(value: TimeResult | undefined) => void> = [];
    mocks.fetchQueryTime.mockImplementation(() => new Promise<TimeResult | undefined>(resolve => {
      finishTime.push(resolve);
    }));
    mocks.fetchLogs.mockResolvedValue({ status: "success" });
    baseTriggers = { ...initialBase, query: "_time:7d" };

    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();

    hitsTriggers = { ...initialHits, step: "1h" };
    rerender();
    await flushDebounce();
    expect(mocks.fetchQueryTime).toHaveBeenCalledTimes(2);
    expect(mocks.fetchLogs).not.toHaveBeenCalled();
    expect(mocks.fetchHits).not.toHaveBeenCalled();

    mocks.dispatch.mockClear();
    await act(async () => { finishTime[0](undefined); });
    expect(mocks.dispatch).not.toHaveBeenCalled();

    hitsTriggers = { ...initialHits, step: "2h" };
    rerender();
    await flushDebounce();
    expect(mocks.fetchQueryTime).toHaveBeenCalledTimes(3);
    expect(mocks.fetchLogs).not.toHaveBeenCalled();

    const resolvedPeriod = {
      start: initialBase.period.start - 86_400_000_000_000n,
      end: initialBase.period.end,
    };
    await act(async () => { finishTime[2]({ ...resolvedPeriod, hasTimeFilter: true }); });
    expect(baseTriggers.setPeriod).toHaveBeenCalledTimes(1);
    expect(mocks.fetchLogs).not.toHaveBeenCalled();

    baseTriggers = { ...baseTriggers, period: resolvedPeriod };
    rerender();
    await flushDebounce();
    expect(mocks.fetchQueryTime).toHaveBeenCalledTimes(3);
    expect(mocks.fetchLogs).toHaveBeenCalledWith(expect.objectContaining({ period: resolvedPeriod }));
    expect(mocks.fetchHits).toHaveBeenCalledWith(expect.objectContaining({ period: resolvedPeriod, step: "2h" }));

    await act(async () => { finishTime[1](undefined); });
    expect(baseTriggers.setPeriod).toHaveBeenCalledTimes(1);
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    await completeHits();

    hitsTriggers = { ...initialHits, step: "3h" };
    rerender();
    await flushDebounce();
    expect(mocks.fetchQueryTime).toHaveBeenCalledTimes(3);
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    await completeHits();
  });

  it("does not refetch time on chart changes after returning to an already-synced time key", async () => {
    type TimeResult = typeof initialBase.period & { hasTimeFilter: boolean };
    let finishStaleTime!: (value: TimeResult | undefined) => void;
    const resolvedPeriod = {
      start: initialBase.period.start - 86_400_000_000_000n,
      end: initialBase.period.end,
    };
    mocks.fetchQueryTime
      .mockResolvedValueOnce({ ...resolvedPeriod, hasTimeFilter: true })
      .mockImplementationOnce(() => new Promise<TimeResult | undefined>(resolve => {
        finishStaleTime = resolve;
      }));
    mocks.fetchLogs.mockResolvedValue({ status: "success" });
    baseTriggers = { ...initialBase, query: "_time:7d" };
    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    expect(baseTriggers.setPeriod).toHaveBeenCalledTimes(1);
    expect(mocks.fetchLogs).not.toHaveBeenCalled();

    baseTriggers = { ...initialBase, query: "_time:1d" };
    rerender();
    await flushDebounce();
    expect(mocks.fetchQueryTime).toHaveBeenCalledTimes(2);

    baseTriggers = { ...initialBase, query: "_time:7d", period: resolvedPeriod };
    rerender();
    await flushDebounce();
    expect(mocks.fetchQueryTime).toHaveBeenCalledTimes(2);
    expect(mocks.fetchLogs).toHaveBeenCalledWith(expect.objectContaining({
      query: "_time:7d", period: resolvedPeriod,
    }));
    await completeHits();

    mocks.dispatch.mockClear();
    await act(async () => { finishStaleTime(undefined); });
    expect(mocks.dispatch).not.toHaveBeenCalled();

    hitsTriggers = { ...initialHits, step: "1h" };
    rerender();
    await flushDebounce();
    expect(mocks.fetchQueryTime).toHaveBeenCalledTimes(2);
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(mocks.fetchHits).toHaveBeenLastCalledWith(expect.objectContaining({
      step: "1h", period: resolvedPeriod,
    }));
    await completeHits();
  });

  it("loads hits without retrying logs when loading logs is declined", async () => {
    mocks.fetchLogs.mockResolvedValue({ status: "aborted" });
    const { result } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();

    expect(mocks.fetchHits).toHaveBeenCalledTimes(1);
    expect(mocks.fetchHits).toHaveBeenCalledWith(expect.objectContaining({ isIterative: false }));
    expect(result.current.logsRequestState.isPending).toBe(false);

    await completeHits();

    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
    expect(result.current.logsRequestState.isPending).toBe(false);
  });

  it.each(["aborted", "success"])("does not continue a cancelled logs request returning %s", async (status) => {
    let finishLogs!: (result: { status: string }) => void;
    mocks.fetchLogs.mockImplementation(() => new Promise(resolve => { finishLogs = resolve; }));
    const { result } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await act(async () => {
      result.current.cancelAll();
      finishLogs({ status });
    });
    expect(mocks.fetchHits).not.toHaveBeenCalled();
    expect(mocks.fetchLogs).toHaveBeenCalledTimes(1);
  });
});
