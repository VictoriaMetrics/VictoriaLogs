import { act, render, renderHook, waitFor } from "@testing-library/preact";
import { FC } from "preact/compat";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useGroupByFields } from "./useGroupByFields";
import { OverviewStateProvider, useOverviewState } from "../state/overview/OverviewStateContext";
import { addRecentGroupByField } from "../utils/groupByFields";
import { WITHOUT_GROUPING } from "../constants/logs";

const routerMock = vi.hoisted(() => ({
  search: "",
  setSearchParams: vi.fn(),
}));

vi.mock("react-router-dom", () => ({
  useSearchParams: () => [new URLSearchParams(routerMock.search), routerMock.setSearchParams],
}));

vi.mock("../state/common/StateContext", () => ({
  useAppState: () => ({ serverUrl: "http://localhost:8080" }),
}));

vi.mock("./useTenant", () => {
  const tenant = { AccountID: "0", ProjectID: "0" };
  return { useTenant: () => tenant };
});

vi.mock("../components/ExtraFilters/hooks/useExtraFilters", () => {
  const extraParams = new URLSearchParams();
  return { useExtraFilters: () => ({ extraParams }) };
});

vi.mock("../pages/QueryPage/hooks/useTimePeriod", async () => {
  const { useState } = await import("preact/hooks");
  let calls = 0;
  // Like the real hook, every component resolves its own end time.
  return {
    useTimePeriod: () => {
      const [period] = useState(() => ({ start: BigInt(0), end: BigInt(3600 + calls++) }));
      return { period };
    },
  };
});

const tenant = { AccountID: "0", ProjectID: "0" };

const wrapper: FC = ({ children }) => <OverviewStateProvider>{children}</OverviewStateProvider>;

const jsonResponse = (values: string[]) => ({
  ok: true,
  json: async () => ({ values: values.map(value => ({ value, hits: 1 })) }),
});

describe("useGroupByFields", () => {
  beforeEach(() => {
    window.localStorage.clear();
    routerMock.search = "";
    routerMock.setSearchParams.mockClear();
    vi.spyOn(console, "error").mockImplementation(() => undefined);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("keeps the options available while loading and merges the fetched fields", async () => {
    let resolveFetch: (values: string[]) => void = () => undefined;
    const fetchMock = vi.fn(() => new Promise(resolve => {
      resolveFetch = (values) => resolve(jsonResponse(values));
    }));
    vi.stubGlobal("fetch", fetchMock);
    addRecentGroupByField(tenant, "level");

    const { result } = renderHook(() => useGroupByFields({ query: "error", loadedFieldNames: ["host"] }), { wrapper });

    act(() => {
      result.current.loadFields();
    });

    expect(result.current.isLoading).toBe(true);
    expect(result.current.options).toEqual([WITHOUT_GROUPING, "level", "host"]);

    await act(async () => {
      resolveFetch(["_time", "app", "host"]);
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.options).toEqual([WITHOUT_GROUPING, "level", "app", "host"]);

    const body = (fetchMock.mock.calls[0] as unknown as [string, RequestInit])[1].body as URLSearchParams;
    expect(body.get("query")).toBe("error");
  });

  it("does not show the previous period's fields while loading or after a failed request", async () => {
    let failRequest: () => void = () => undefined;
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse(["old_field"]))
      .mockImplementationOnce(() => new Promise(resolve => {
        failRequest = () => resolve({
          ok: false,
          status: 500,
          statusText: "Internal Server Error",
          text: async () => "boom",
        });
      }));
    vi.stubGlobal("fetch", fetchMock);

    let period = { start: BigInt(0), end: BigInt(100) };
    const { result, rerender } = renderHook(() => useGroupByFields({ period, loadedFieldNames: ["host"] }), { wrapper });

    await act(async () => {
      result.current.loadFields();
    });
    await waitFor(() => expect(result.current.options).toContain("old_field"));

    period = { start: BigInt(100), end: BigInt(200) };
    rerender();
    act(() => {
      result.current.loadFields();
    });

    expect(result.current.isLoading).toBe(true);
    expect(result.current.options).toEqual([WITHOUT_GROUPING, "host"]);

    await act(async () => {
      failRequest();
    });

    await waitFor(() => expect(result.current.error).not.toBe(""));
    expect(result.current.options).toEqual([WITHOUT_GROUPING, "host"]);
  });

  it("does not show a response for the previous period after the period changes", async () => {
    let resolveFetch: (values: string[]) => void = () => undefined;
    vi.stubGlobal("fetch", vi.fn(() => new Promise(resolve => {
      resolveFetch = (values) => resolve(jsonResponse(values));
    })));

    let period = { start: BigInt(0), end: BigInt(100) };
    const { result, rerender } = renderHook(() => useGroupByFields({ period, loadedFieldNames: ["host"] }), { wrapper });

    act(() => {
      result.current.loadFields();
    });

    period = { start: BigInt(100), end: BigInt(200) };
    rerender();

    await act(async () => {
      resolveFetch(["old_field"]);
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.options).toEqual([WITHOUT_GROUPING, "host"]);
  });

  it("shares a single request between selectors with the same period", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(["host", "level"]));
    vi.stubGlobal("fetch", fetchMock);
    const period = { start: BigInt(100), end: BigInt(200) };

    const { result } = renderHook(() => ({
      hits: useGroupByFields({ query: "*", period }),
      groupView: useGroupByFields({ query: "*", period, loadedFieldNames: ["host"] }),
    }), { wrapper });

    await act(async () => {
      result.current.groupView.loadFields();
    });
    await waitFor(() => expect(result.current.groupView.isLoading).toBe(false));

    await act(async () => {
      result.current.hits.loadFields();
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(result.current.hits.options).toEqual(result.current.groupView.options);
    expect(result.current.hits.options).toEqual([WITHOUT_GROUPING, "host", "level"]);
  });

  it("shows the cached fields on the first render, before loadFields is called", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(["host", "level"])));
    const period = { start: BigInt(100), end: BigInt(200) };
    let hits: ReturnType<typeof useGroupByFields> | undefined;
    const groupViewRenders: string[][] = [];

    const HitsSelector = () => {
      hits = useGroupByFields({ period });
      return null;
    };
    const GroupViewSelector = () => {
      groupViewRenders.push(useGroupByFields({ period }).options);
      return null;
    };
    const Page = ({ showGroupView }: { showGroupView: boolean }) => (
      <OverviewStateProvider>
        <HitsSelector/>
        {showGroupView && <GroupViewSelector/>}
      </OverviewStateProvider>
    );

    const { rerender } = render(<Page showGroupView={false}/>);

    await act(async () => {
      hits?.loadFields();
    });
    await waitFor(() => expect(hits?.isLoading).toBe(false));

    rerender(<Page showGroupView={true}/>);

    expect(groupViewRenders[0]).toEqual([WITHOUT_GROUPING, "host", "level"]);
  });

  it("syncs recent fields selected in another selector", () => {
    const { result } = renderHook(() => ({
      hits: useGroupByFields({}),
      groupView: useGroupByFields({}),
    }), { wrapper });

    act(() => {
      result.current.hits.selectField("host");
    });

    expect(result.current.groupView.options).toEqual([WITHOUT_GROUPING, "host"]);
  });

  it("does not change the latest field names shown by the Overview field table", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(["host"])));

    const { result } = renderHook(() => ({
      groupBy: useGroupByFields({}),
      state: useOverviewState(),
    }), { wrapper });

    await act(async () => {
      result.current.groupBy.loadFields();
    });
    await waitFor(() => expect(result.current.groupBy.options).toEqual([WITHOUT_GROUPING, "host"]));

    expect(result.current.state.fieldNamesParamsKey).toBeNull();
    expect(result.current.state.fieldNamesCache.size).toBe(1);
  });
});
