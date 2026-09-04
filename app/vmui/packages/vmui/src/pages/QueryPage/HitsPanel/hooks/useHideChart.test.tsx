import { act, renderHook, waitFor } from "@testing-library/preact";
import { useHideChart } from "./useHideChart";

const STORAGE_KEY = "VLUI:LOGS_HIDE_CHART";

const routerMock = vi.hoisted(() => ({
  search: "",
  setSearchParams: vi.fn(),
}));

vi.mock("react-router-dom", () => ({
  useSearchParams: () => [new URLSearchParams(routerMock.search), routerMock.setSearchParams],
}));

describe("useHideChart", () => {
  beforeEach(() => {
    window.localStorage.clear();
    routerMock.search = "";
    routerMock.setSearchParams.mockClear();
  });

  it.each([
    { url: "/", stored: false, expected: false },
    { url: "/?hide_chart=true", stored: false, expected: true },
    { url: "/", stored: true, expected: true },
    { url: "/?hide_chart=true", stored: true, expected: true },
  ])("returns $expected for $url with stored=$stored", ({ url, stored, expected }) => {
    routerMock.search = new URL(url, "http://localhost").search;

    if (stored) {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify({ value: true }));
    }

    const { result } = renderHook(() => useHideChart());

    expect(result.current[0]).toBe(expected);
  });

  it("updates the URL-backed state and localStorage", async () => {
    const { result } = renderHook(() => useHideChart());

    act(() => result.current[1](true));

    await waitFor(() => expect(result.current[0]).toBe(true));
    expect(window.localStorage.getItem(STORAGE_KEY)).toBe(JSON.stringify({ value: true }));
    expect(routerMock.setSearchParams.mock.calls[0][0].get("hide_chart")).toBe("true");

    act(() => result.current[1](false));

    await waitFor(() => expect(result.current[0]).toBe(false));
    expect(window.localStorage.getItem(STORAGE_KEY)).toBeNull();
    expect(routerMock.setSearchParams.mock.calls[1][0].has("hide_chart")).toBe(false);
  });
});
