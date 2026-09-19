import { act, cleanup, renderHook } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useQueryPageController } from "./index";

const mocks = vi.hoisted(() => ({
  runHits: vi.fn(),
  runLogs: vi.fn(),
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
let finishHits: ((success: boolean) => void) | undefined;

vi.mock("../useQueryPageTriggers/", () => ({
  useBaseTriggers: () => baseTriggers,
  useHitsTriggers: () => hitsTriggers,
  useLogsTriggers: () => logsTriggers,
}));
vi.mock("./useHitsController", () => ({
  useHitsController: () => ({ runHits: mocks.runHits, abort: mocks.abortHits }),
}));
vi.mock("./useLogsController", () => ({
  useLogsController: () => ({ runLogs: mocks.runLogs, abort: mocks.abortLogs }),
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

const completeHits = async () => {
  expect(finishHits).toBeDefined();
  await act(async () => {
    finishHits!(true);
    finishHits = undefined;
  });
};

describe("useQueryPageController: logs waiting for hits", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.resetAllMocks();
    baseTriggers = initialBase;
    hitsTriggers = initialHits;
    finishHits = undefined;
    mocks.fetchQueryTime.mockResolvedValue(undefined);
    mocks.runLogs.mockResolvedValue(true);
    mocks.abortHits.mockImplementation(() => {
      finishHits?.(false);
      finishHits = undefined;
    });
    // Match useFetchHits: starting another hits request cancels the previous one.
    mocks.runHits.mockImplementation(() => {
      mocks.abortHits();
      return new Promise<boolean>(resolve => {
        finishHits = resolve;
      });
    });
  });

  afterEach(() => {
    cleanup();
    finishHits?.(false);
    finishHits = undefined;
    vi.clearAllTimers();
    vi.useRealTimers();
  });

  it("loads logs only after hits finish", async () => {
    renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();

    expect(mocks.runHits).toHaveBeenCalledTimes(1);
    expect(mocks.runLogs).not.toHaveBeenCalled();

    await completeHits();

    expect(mocks.runLogs).toHaveBeenCalledTimes(1);
    expect(mocks.runLogs).toHaveBeenCalledWith(expect.objectContaining({ query: "*" }));
  });

  it("loads the new query's logs after changing step while its hits are pending", async () => {
    const { rerender } = renderHook(() => useQueryPageController({ query: baseTriggers.query }));
    await flushDebounce();
    await completeHits();
    expect(mocks.runLogs).toHaveBeenCalledTimes(1);

    baseTriggers = { ...initialBase, query: "_msg:~\"(?i)(error|warning)\"" };
    rerender();
    await flushDebounce();
    expect(mocks.runHits).toHaveBeenCalledTimes(2);
    expect(mocks.runLogs).toHaveBeenCalledTimes(1);

    // Only hits settings change; the query, period and logs settings stay the same.
    hitsTriggers = { ...initialHits, step: "1h" };
    rerender();
    await flushDebounce();
    expect(mocks.runHits).toHaveBeenCalledTimes(3);
    expect(mocks.runHits).toHaveBeenLastCalledWith(expect.objectContaining({
      query: baseTriggers.query,
      step: "1h",
    }));
    expect(mocks.runLogs).toHaveBeenCalledTimes(1);

    await completeHits();

    expect(mocks.runLogs).toHaveBeenCalledTimes(2);
    expect(mocks.runLogs).toHaveBeenLastCalledWith(expect.objectContaining({
      query: baseTriggers.query,
      period: baseTriggers.period,
    }));
  });
});
