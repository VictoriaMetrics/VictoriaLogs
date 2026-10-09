import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, fireEvent, act, screen } from "@testing-library/preact";
import LiveTailingView from "./LiveTailingView";
import { useLiveTailingLogs } from "./useLiveTailingLogs";
import { Logs } from "../../../api/types";

vi.mock("./useLiveTailingLogs", () => ({
  useLiveTailingLogs: vi.fn(),
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

describe("LiveTailingView scrolling behavior (issue #1842 baseline reproduction)", () => {
  let mockLiveLogsState: ReturnType<typeof useLiveTailingLogs>;
  let scrollToSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.useFakeTimers();
    mockRawJsonView = false;
    scrollToSpy = vi.spyOn(window, "scrollTo").mockImplementation(() => {});
    window.scrollY = 0;
    document.documentElement.scrollTop = 0;
    Object.defineProperty(document.documentElement, "scrollHeight", { configurable: true, value: 2000 });
    Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 800 });

    mockLiveLogsState = {
      logs: [],
      isPaused: false,
      error: undefined,
      startLiveTailing: vi.fn().mockResolvedValue(true),
      stopLiveTailing: vi.fn().mockResolvedValue(undefined),
      pauseLiveTailing: vi.fn(),
      resumeLiveTailing: vi.fn(),
      clearLogs: vi.fn(),
      isLimitedLogsPerUpdate: false,
    };

    vi.mocked(useLiveTailingLogs).mockImplementation(() => mockLiveLogsState);
  });

  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("scrolls to document bottom on initial mount with empty logs", () => {
    render(
      <LiveTailingView
        data={[]}
        settingsRef={{ current: null }}
      />
    );
    act(() => {
      vi.advanceTimersByTime(300);
    });

    // Reproduces issue #1842: mount unconditionally scrolls to document.documentElement.scrollHeight
    expect(scrollToSpy).toHaveBeenCalledWith({
      top: 2000,
      behavior: "smooth",
    });
    expect(mockLiveLogsState.startLiveTailing).toHaveBeenCalled();
    expect(screen.getByText("Waiting for logs...")).toBeInTheDocument();
  });

  it("renders incoming logs in grouped mode and scrolls to document bottom", () => {
    mockLiveLogsState.logs = [createSampleLog("1", "first log")];

    render(
      <LiveTailingView
        data={[]}
        settingsRef={{ current: null }}
      />
    );
    act(() => {
      vi.advanceTimersByTime(300);
    });

    expect(screen.getByTestId("group-log-item")).toBeInTheDocument();
    expect(scrollToSpy).toHaveBeenCalledWith({
      top: 2000,
      behavior: "smooth",
    });
  });

  it("pauses live tailing on upward scroll based on document height", () => {
    mockLiveLogsState.logs = [createSampleLog("1", "first log")];

    render(
      <LiveTailingView
        data={[]}
        settingsRef={{ current: null }}
      />
    );

    document.documentElement.scrollTop = 500;
    fireEvent.scroll(document);
    document.documentElement.scrollTop = 400;
    fireEvent.scroll(document);
    document.documentElement.scrollTop = 300;
    fireEvent.scroll(document);

    expect(mockLiveLogsState.pauseLiveTailing).toHaveBeenCalled();
  });
});
