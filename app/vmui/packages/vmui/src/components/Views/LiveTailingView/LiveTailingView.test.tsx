import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, fireEvent, act, screen } from "@testing-library/preact";
import { useState, useEffect, useCallback } from "preact/compat";
import LiveTailingView from "./LiveTailingView";
import { Logs } from "../../../api/types";

interface MockHookState {
  logs: Logs[];
  isPaused: boolean;
  error?: string;
  isLimitedLogsPerUpdate: boolean;
}

let currentHookState: MockHookState;
let updateHookState: ((partial: Partial<MockHookState>) => void) | null = null;
const pauseLiveTailingSpy = vi.fn();
const resumeLiveTailingSpy = vi.fn();
const startLiveTailingSpy = vi.fn();
const stopLiveTailingSpy = vi.fn();
const clearLogsSpy = vi.fn();

vi.mock("./useLiveTailingLogs", () => ({
  useLiveTailingLogs: () => {
    const [state, setState] = useState<MockHookState>(currentHookState);

    useEffect(() => {
      updateHookState = (partial: Partial<MockHookState>) => {
        act(() => {
          setState((prev) => ({ ...prev, ...partial }));
        });
      };
      return () => {
        updateHookState = null;
      };
    }, []);

    const pauseLiveTailing = useCallback(() => {
      pauseLiveTailingSpy();
      setState((prev) => ({ ...prev, isPaused: true }));
    }, []);

    const resumeLiveTailing = useCallback(() => {
      resumeLiveTailingSpy();
      setState((prev) => ({ ...prev, isPaused: false }));
    }, []);

    return {
      logs: state.logs,
      isPaused: state.isPaused,
      error: state.error,
      startLiveTailing: startLiveTailingSpy,
      stopLiveTailing: stopLiveTailingSpy,
      pauseLiveTailing,
      resumeLiveTailing,
      clearLogs: clearLogsSpy,
      isLimitedLogsPerUpdate: state.isLimitedLogsPerUpdate,
    };
  },
}));

vi.mock("../GroupView/GroupLogsItem", () => ({
  default: ({ log }: { log: Record<string, unknown> }) => <div data-testid="group-log-item">{JSON.stringify(log)}</div>,
}));

const mockSetSearchParamsFromKeys = vi.fn();
vi.mock("../../../hooks/useSearchParamsFromObject", () => ({
  default: () => ({ setSearchParamsFromKeys: mockSetSearchParamsFromKeys }),
}));

vi.mock("../../../hooks/useStateSearchParams", () => ({
  default: (initial: unknown) => [initial, vi.fn()],
}));

vi.mock("react-router-dom", () => ({
  useSearchParams: () => [new URLSearchParams(), vi.fn()],
}));

let mockRawJsonView = false;
vi.mock("../../../hooks/useLocalStorageBoolean", () => ({
  useLocalStorageBoolean: () => [mockRawJsonView, vi.fn()],
}));

const createSampleLog = (id: string, msg: string): Logs => ({
  _msg: msg,
  _stream: "{}",
  _time: "2026-10-09T00:00:00Z",
  _log_id: id,
});

const setScrollPosition = (y: number) => {
  window.scrollY = y;
  document.documentElement.scrollTop = y;
};

const createGeometryHelper = () => {
  let headerRect: DOMRect = {
    top: 0,
    bottom: 48,
    height: 48,
    width: 1000,
    left: 0,
    right: 1000,
    x: 0,
    y: 0,
    toJSON: () => {},
  };

  let logsRect: DOMRect = {
    top: 48,
    bottom: 300,
    height: 252,
    width: 1000,
    left: 0,
    right: 1000,
    x: 0,
    y: 48,
    toJSON: () => {},
  };

  const defaultRect: DOMRect = {
    top: 0,
    bottom: 0,
    height: 0,
    width: 0,
    left: 0,
    right: 0,
    x: 0,
    y: 0,
    toJSON: () => {},
  };

  vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(function (
    this: HTMLElement
  ) {
    if (this.classList.contains("vm-query-page-body-header")) {
      return { ...headerRect, x: headerRect.left, y: headerRect.top, toJSON: () => {} } as DOMRect;
    }
    if (this.classList.contains("vm-live-tailing-view__logs")) {
      return { ...logsRect, x: logsRect.left, y: logsRect.top, toJSON: () => {} } as DOMRect;
    }
    return defaultRect;
  });

  return {
    setHeader: (rect: Partial<DOMRect>) => {
      headerRect = { ...headerRect, ...rect };
    },
    setLogs: (rect: Partial<DOMRect>) => {
      logsRect = { ...logsRect, ...rect };
    },
  };
};

const renderHarness = () => {
  const settingsSlot = document.createElement("div");
  const settingsRef = { current: settingsSlot };

  const result = render(
    <div className="vm-query-page-body">
      <div className="vm-query-page-body-header">
        <div ref={(el) => { if (el && !el.contains(settingsSlot)) el.appendChild(settingsSlot); }} />
      </div>
      <div className="vm-query-page-body__content">
        <LiveTailingView
          data={[]}
          settingsRef={settingsRef}
        />
      </div>
    </div>
  );

  return { ...result, settingsSlot, settingsRef };
};

describe("LiveTailingView scrolling behavior (issue #1842)", () => {
  let scrollToSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.useFakeTimers();
    mockRawJsonView = false;
    scrollToSpy = vi.spyOn(window, "scrollTo").mockImplementation(() => {});
    setScrollPosition(0);
    Object.defineProperty(window, "innerHeight", { configurable: true, value: 800 });

    currentHookState = {
      logs: [],
      isPaused: false,
      error: undefined,
      isLimitedLogsPerUpdate: false,
    };

    pauseLiveTailingSpy.mockClear();
    resumeLiveTailingSpy.mockClear();
    startLiveTailingSpy.mockClear().mockResolvedValue(true);
    stopLiveTailingSpy.mockClear().mockResolvedValue(undefined);
    clearLogsSpy.mockClear();
  });

  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("does not scroll on initial mount with empty logs and starts live tailing", () => {
    createGeometryHelper();
    renderHarness();
    act(() => {
      vi.advanceTimersByTime(300);
    });

    expect(scrollToSpy).not.toHaveBeenCalled();
    expect(startLiveTailingSpy).toHaveBeenCalled();
    expect(screen.getByText("Waiting for logs...")).toBeInTheDocument();
  });

  it("transitions from empty to incoming logs without jumping when logs fit", () => {
    const geometry = createGeometryHelper();
    renderHarness();
    act(() => {
      vi.advanceTimersByTime(300);
    });
    expect(scrollToSpy).not.toHaveBeenCalled();

    // Incoming logs that fit within the viewport (top: 48, bottom: 300 <= window.innerHeight 800)
    geometry.setLogs({ top: 48, bottom: 300 });
    updateHookState?.({
      logs: [createSampleLog("1", "first log"), createSampleLog("2", "second log")],
    });
    act(() => {
      vi.advanceTimersByTime(300);
    });

    expect(screen.queryByText("Waiting for logs...")).toBeNull();
    const renderedItems = screen.getAllByTestId("group-log-item");
    expect(renderedItems).toHaveLength(2);
    expect(renderedItems[0].textContent).toContain("first log");
    expect(renderedItems[1].textContent).toContain("second log");
    expect(scrollToSpy).not.toHaveBeenCalled();
  });

  it("auto-follows incoming logs when they overflow the visible viewport", () => {
    const geometry = createGeometryHelper();
    geometry.setLogs({ top: 100, bottom: 1200 });
    currentHookState.logs = [createSampleLog("1", "overflowing log")];

    renderHarness();
    act(() => {
      vi.advanceTimersByTime(300);
    });

    // Positions rect.bottom at window.innerHeight: scrollY (0) + 1200 - 800 = 400
    expect(scrollToSpy).toHaveBeenCalledWith({
      top: 400,
      behavior: "smooth",
    });
  });

  it("recovers scroll when logs are hidden beneath the sticky header", () => {
    const geometry = createGeometryHelper();
    // Sticky header bottom is 48; logs bottom at 40 is behind/above the sticky header
    geometry.setHeader({ top: 0, bottom: 48 });
    geometry.setLogs({ top: -100, bottom: 40 });
    currentHookState.logs = [createSampleLog("1", "partially hidden log")];
    setScrollPosition(600);

    renderHarness();
    act(() => {
      vi.advanceTimersByTime(300);
    });

    // Target: Math.max(0, scrollY (600) + rect.bottom (40) - innerHeight (800)) = 0
    expect(scrollToSpy).toHaveBeenCalledWith({
      top: 0,
      behavior: "instant",
    });
  });

  it("causes no scroll when logs are already visible within usable viewport", () => {
    const geometry = createGeometryHelper();
    // Header bottom is 48; logs bottom is 500 (> 48 and <= window.innerHeight 800)
    geometry.setHeader({ top: 0, bottom: 48 });
    geometry.setLogs({ top: 48, bottom: 500 });
    currentHookState.logs = [createSampleLog("1", "visible log")];

    renderHarness();
    act(() => {
      vi.advanceTimersByTime(300);
    });

    expect(scrollToSpy).not.toHaveBeenCalled();
  });

  it("recovers immediately on Resume and keeps following a batch arriving before the scroll event", () => {
    const geometry = createGeometryHelper();
    geometry.setHeader({ top: 0, bottom: 48 });
    geometry.setLogs({ top: 48, bottom: 400 });
    currentHookState.logs = [createSampleLog("1", "tailing log")];

    renderHarness();

    // User pauses live tailing
    const pauseBtn = screen.getByRole("button", { name: "Pause live tailing" });
    fireEvent.click(pauseBtn);
    act(() => {
      vi.advanceTimersByTime(500);
    });

    const resumeBtn = screen.getByRole("button", { name: "Resume live tailing" });
    expect(resumeBtn).toBeInTheDocument();

    // Scrolled down: scrollY = 600, logs are positioned above the usable viewport
    setScrollPosition(600);
    geometry.setLogs({ top: -300, bottom: -150 });

    scrollToSpy.mockClear();
    pauseLiveTailingSpy.mockClear();
    resumeLiveTailingSpy.mockClear();

    // User clicks Resume
    fireEvent.click(resumeBtn);
    act(() => {});

    expect(resumeLiveTailingSpy).toHaveBeenCalled();
    expect(scrollToSpy).toHaveBeenCalledWith({
      top: 0,
      behavior: "instant",
    });

    // Recovery lands immediately; a batch can arrive before its scroll event is delivered.
    setScrollPosition(0);
    geometry.setLogs({ top: 48, bottom: 1000 });
    updateHookState?.({
      logs: [createSampleLog("1", "tailing log"), createSampleLog("2", "new batch")],
    });
    fireEvent.scroll(document);
    act(() => {
      vi.advanceTimersByTime(300);
    });

    expect(scrollToSpy).toHaveBeenLastCalledWith({ top: 200, behavior: "smooth" });
    expect(pauseLiveTailingSpy).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Pause live tailing" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Resume live tailing" })).toBeNull();
  });

  it("pauses live tailing on manual upward scroll and resumes when requested", () => {
    const geometry = createGeometryHelper();
    // Latest logs extend below the viewport lower boundary: rect.bottom = 1400 (> 800 + 100)
    geometry.setLogs({ top: 400, bottom: 1400 });
    currentHookState.isPaused = false;
    currentHookState.logs = [createSampleLog("1", "streamed log")];
    setScrollPosition(600);

    renderHarness();

    // User manually scrolls up away from latest logs
    setScrollPosition(500);
    fireEvent.scroll(document);
    setScrollPosition(400);
    fireEvent.scroll(document);
    setScrollPosition(300);
    fireEvent.scroll(document);

    expect(pauseLiveTailingSpy).toHaveBeenCalled();
    const resumeBtn = screen.getByRole("button", { name: "Resume live tailing" });
    expect(resumeBtn).toBeInTheDocument();

    // Resuming scrolls down to bring the latest logs back to the bottom
    scrollToSpy.mockClear();
    fireEvent.click(resumeBtn);
    act(() => {
      vi.advanceTimersByTime(300);
    });

    expect(resumeLiveTailingSpy).toHaveBeenCalled();
    // scrollY (300) + 1400 - 800 = 900
    expect(scrollToSpy).toHaveBeenCalledWith({
      top: 900,
      behavior: "smooth",
    });
    expect(screen.getByRole("button", { name: "Pause live tailing" })).toBeInTheDocument();
  });

  it("ensures no scrolling occurs after unmount even if a throttled update was scheduled", () => {
    const geometry = createGeometryHelper();
    geometry.setLogs({ top: 100, bottom: 1200 });
    currentHookState.logs = [createSampleLog("1", "log")];

    const { unmount } = renderHarness();

    // Initial mount executed leading throttled call
    expect(scrollToSpy).toHaveBeenCalledTimes(1);

    // Queue a trailing throttled call within the 200ms throttle interval
    updateHookState?.({
      logs: [createSampleLog("1", "log"), createSampleLog("2", "next log")],
    });
    expect(scrollToSpy).toHaveBeenCalledTimes(1);

    // Unmount component before throttle timer triggers
    unmount();

    // Advance timers past the throttle window
    act(() => {
      vi.advanceTimersByTime(500);
    });

    // Observable guarantee: no scroll invocation occurred after unmount
    expect(scrollToSpy).toHaveBeenCalledTimes(1);
    expect(stopLiveTailingSpy).toHaveBeenCalled();
  });

  it.each([
    { isRawJson: false, mode: "grouped" },
    { isRawJson: true, mode: "raw JSON" },
  ])("renders and auto-follows in $mode mode", ({ isRawJson }) => {
    mockRawJsonView = isRawJson;
    const geometry = createGeometryHelper();
    geometry.setLogs({ top: 100, bottom: 1100 });
    currentHookState.logs = [createSampleLog("1", `${isRawJson ? "raw" : "grouped"} log entry`)];

    const { container } = renderHarness();
    act(() => {
      vi.advanceTimersByTime(300);
    });

    if (isRawJson) {
      const rawRow = container.querySelector(".vm-live-tailing-view__log-row");
      expect(rawRow).not.toBeNull();
      expect(rawRow?.textContent).toContain("raw log entry");
      expect(screen.queryByTestId("group-log-item")).toBeNull();
    } else {
      expect(screen.getByTestId("group-log-item")).toBeInTheDocument();
      expect(container.querySelector(".vm-live-tailing-view__log-row")).toBeNull();
    }

    expect(scrollToSpy).toHaveBeenCalledWith({
      top: 300,
      behavior: "smooth",
    });
  });
});
