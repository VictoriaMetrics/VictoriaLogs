import { act, renderHook, waitFor } from "@testing-library/preact";
import { FC } from "preact/compat";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useFetchFieldNames } from "./useFetchFieldNames";
import { OverviewStateProvider } from "../../../state/overview/OverviewStateContext";
import { TimeParams } from "../../../types";

vi.mock("../../../state/common/StateContext", () => ({
  useAppState: () => ({ serverUrl: "http://localhost:8080" }),
}));

vi.mock("../../../hooks/useTenant", () => {
  // The real hook memoizes the tenant.
  const tenant = { AccountID: "0", ProjectID: "0" };
  return { useTenant: () => tenant };
});

const wrapper: FC = ({ children }) => <OverviewStateProvider>{children}</OverviewStateProvider>;

const jsonResponse = (values: string[]) => ({
  ok: true,
  json: async () => ({ values: values.map(value => ({ value, hits: 1 })) }),
});

const createDeferredFetch = () => {
  const pending: { resolve: (values: string[]) => void; signal: AbortSignal }[] = [];

  const fetchImpl = vi.fn((_url: string, init: RequestInit) => new Promise((resolve, reject) => {
    const signal = init.signal as AbortSignal;
    signal.addEventListener("abort", () => reject(new DOMException("The operation was aborted.", "AbortError")));
    pending.push({ resolve: (values) => resolve(jsonResponse(values)), signal });
  }));

  return { fetchImpl, pending };
};

const period = (start: number): TimeParams => ({ start: BigInt(start), end: BigInt(start + 3600) });

describe("useFetchFieldNames", () => {
  beforeEach(() => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("serves repeated requests from the cache", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse(["a"]))
      .mockResolvedValueOnce(jsonResponse(["b"]));
    vi.stubGlobal("fetch", fetchMock);

    const { result } = renderHook(() => useFetchFieldNames(), { wrapper });

    await act(async () => {
      await result.current.fetchFieldNames({ period: period(0) });
    });
    await act(async () => {
      await result.current.fetchFieldNames({ period: period(100) });
    });
    await act(async () => {
      await result.current.fetchFieldNames({ period: period(0) });
    });

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(result.current.fieldNames.map(f => f.value)).toEqual(["a"]);
  });

  it("keeps fetchFieldNames stable when the cache changes", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(["a"])));

    const { result } = renderHook(() => useFetchFieldNames(), { wrapper });
    const initialFetchFieldNames = result.current.fetchFieldNames;

    await act(async () => {
      await result.current.fetchFieldNames({ period: period(0) });
    });

    expect(result.current.fetchFieldNames).toBe(initialFetchFieldNames);
  });

  it("aborts the previous request and ignores its result", async () => {
    const { fetchImpl, pending } = createDeferredFetch();
    vi.stubGlobal("fetch", fetchImpl);

    const { result } = renderHook(() => useFetchFieldNames(), { wrapper });

    let first: Promise<void> = Promise.resolve();
    let second: Promise<void> = Promise.resolve();
    act(() => {
      first = result.current.fetchFieldNames({ period: period(0) });
    });
    act(() => {
      second = result.current.fetchFieldNames({ period: period(100) });
    });

    expect(pending).toHaveLength(2);
    expect(pending[0].signal.aborted).toBe(true);

    await act(async () => {
      await first;
    });
    expect(result.current.loading).toBe(true);
    expect(result.current.error).toBe("");

    await act(async () => {
      pending[1].resolve(["new"]);
      await second;
    });

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.fieldNames.map(f => f.value)).toEqual(["new"]);
    expect(result.current.error).toBe("");
  });
});
