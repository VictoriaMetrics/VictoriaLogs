import { act, cleanup, renderHook } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useFetchLogs } from "./useFetchLogs";
import type { FetchLogsResult } from "./useFetchLogs";

const mocks = vi.hoisted(() => ({
  tenant: { AccountID: "0", ProjectID: "0" },
  searchParams: new URLSearchParams(),
  setSearchParams: vi.fn(),
}));

vi.mock("../../../hooks/useTenant", () => ({ useTenant: () => mocks.tenant }));
vi.mock("../../../state/common/StateContext", () => ({
  useAppState: () => ({ serverUrl: "http://localhost:9428" }),
}));
vi.mock("react-router-dom", () => ({
  useSearchParams: () => [mocks.searchParams, mocks.setSearchParams],
}));

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("useFetchLogs confirmed limit", () => {
  it.each([false, true])("returns the limit sent after guard modification (aborted=%s)", async aborted => {
    const fetchMock = vi.fn(async (_url, options: RequestInit) => {
      expect((options.body as URLSearchParams).get("limit")).toBe("500");
      if (aborted) {
        return new Promise<Response>((_resolve, reject) => {
          options.signal!.addEventListener("abort", () => {
            reject(Object.assign(new Error("Aborted"), { name: "AbortError" }));
          });
        });
      }
      return new Response(`${JSON.stringify({ _msg: "test" })}\n`);
    });
    vi.stubGlobal("fetch", fetchMock);
    const { result } = renderHook(() => useFetchLogs());
    let response: FetchLogsResult | undefined;
    await act(async () => {
      const request = result.current.fetchLogs({
        query: "*",
        limit: 5000,
        beforeFetch: async () => ({ action: "modify", body: new URLSearchParams({ limit: "500" }) }),
      });
      await Promise.resolve();
      if (aborted) result.current.abort();
      response = await request;
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(response).toMatchObject({ limit: 500, status: aborted ? "aborted" : "success" });
  });
});
