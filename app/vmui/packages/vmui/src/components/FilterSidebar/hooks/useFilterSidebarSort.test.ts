import { act, renderHook } from "@testing-library/preact";
import { afterEach, describe, expect, it } from "vitest";
import { getFromStorage, removeFromStorage, saveToStorage } from "../../../utils/storage";
import { useFilterSidebarSort } from "./useFilterSidebarSort";
import { DEFAULT_FILTER_SIDEBAR_SORT } from "../constants";

const STORAGE_KEY = "LOGS_FILTER_SIDEBAR_SORT";

describe("useFilterSidebarSort", () => {
  afterEach(() => {
    removeFromStorage([STORAGE_KEY]);
  });

  it("uses the default sort when localStorage is empty", () => {
    const { result } = renderHook(() => useFilterSidebarSort());

    expect(result.current.sort).toEqual(DEFAULT_FILTER_SIDEBAR_SORT);
    expect(result.current.sort).toEqual({ by: "name", direction: "asc" });
  });

  it("initializes with the sort from localStorage", () => {
    saveToStorage(STORAGE_KEY, { by: "hits", direction: "desc" });

    const { result } = renderHook(() => useFilterSidebarSort());

    expect(result.current.sort).toEqual({ by: "hits", direction: "desc" });
  });

  it("updates the sort and localStorage", () => {
    const { result } = renderHook(() => useFilterSidebarSort());

    act(() => {
      result.current.setSort({ by: "name", direction: "desc" });
    });

    expect(result.current.sort).toEqual({ by: "name", direction: "desc" });
    expect(getFromStorage(STORAGE_KEY)).toEqual({ by: "name", direction: "desc" });
  });

  it("falls back to the default sort for an invalid stored value", () => {
    saveToStorage(STORAGE_KEY, { by: "invalid", direction: "asc" });

    const { result } = renderHook(() => useFilterSidebarSort());

    expect(result.current.sort).toEqual(DEFAULT_FILTER_SIDEBAR_SORT);
  });

  it("reacts to sort changes from localStorage", () => {
    const { result } = renderHook(() => useFilterSidebarSort());

    act(() => {
      saveToStorage(STORAGE_KEY, { by: "hits", direction: "asc" });
    });

    expect(result.current.sort).toEqual({ by: "hits", direction: "asc" });
  });

  it("shares sort changes between mounted hook instances", () => {
    const first = renderHook(() => useFilterSidebarSort());
    const second = renderHook(() => useFilterSidebarSort());

    act(() => {
      first.result.current.setSort({ by: "hits", direction: "asc" });
    });

    expect(second.result.current.sort).toEqual({ by: "hits", direction: "asc" });

    act(() => {
      second.result.current.setSort({ by: "name", direction: "desc" });
    });

    expect(first.result.current.sort).toEqual({ by: "name", direction: "desc" });
  });
});
