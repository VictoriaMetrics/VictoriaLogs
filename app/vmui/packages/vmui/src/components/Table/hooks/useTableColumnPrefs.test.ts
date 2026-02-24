import { describe, it, expect, beforeEach, vi } from "vitest";
import { renderHook, act, waitFor } from "@testing-library/preact";
import { useTableColumnPrefs } from "./useTableColumnPrefs";

import * as storage from "../../../utils/storage";
import useEventListener from "../../../hooks/useEventListener";

vi.mock("../../../utils/storage", () => ({
  getTableStorageKey: vi.fn((tableKey: string, suffix: string) => `${tableKey}:${suffix}`),
  getFromStorage: vi.fn(),
  saveToStorage: vi.fn(),
}));

vi.mock("../../../hooks/useEventListener", () => ({
  default: vi.fn(),
}));

const mockedGetTableStorageKey = vi.mocked(storage.getTableStorageKey);
const mockedGetFromStorage = vi.mocked(storage.getFromStorage);
const mockedSaveToStorage = vi.mocked(storage.saveToStorage);
const mockedUseEventListener = vi.mocked(useEventListener);

describe("useTableColumnPrefs", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should generate correct storageKey", () => {
    renderHook(() => useTableColumnPrefs("table1"));

    expect(mockedGetTableStorageKey).toHaveBeenCalledWith("table1", "COLS_PREFS");
  });

  it("should load prefs from storage on mount", async () => {
    mockedGetFromStorage.mockReturnValue({
      col1: { width: 100, wrapped: true },
    });

    const { result } = renderHook(() => useTableColumnPrefs("table1"));

    await waitFor(() => {
      expect(result.current.getColumnPrefs("col1")).toEqual({
        width: 100,
        wrapped: true,
      });
    });

    expect(mockedGetFromStorage).toHaveBeenCalledWith("table1:COLS_PREFS");
  });

  it("should return undefined if prefs do not exist", async () => {
    mockedGetFromStorage.mockReturnValue({});

    const { result } = renderHook(() => useTableColumnPrefs("table1"));

    await waitFor(() => {
      expect(result.current.getColumnPrefs("missing")).toBeUndefined();
    });
  });

  it("should ignore invalid stored value", async () => {
    mockedGetFromStorage.mockReturnValue("bad-data");

    const { result } = renderHook(() => useTableColumnPrefs("table1"));

    await waitFor(() => {
      expect(result.current.getColumnPrefs("col1")).toBeUndefined();
    });
  });

  it("should update pref and persist to storage", async () => {
    mockedGetFromStorage.mockReturnValue({});

    const { result } = renderHook(() => useTableColumnPrefs("table1"));

    await act(() => {
      result.current.updateColumnPref("col1", "width", 250);
    });

    await waitFor(() => {
      expect(result.current.getColumnPrefs("col1")).toEqual({ width: 250 });
    });

    expect(mockedSaveToStorage).toHaveBeenCalledWith("table1:COLS_PREFS", {
      col1: { width: 250 },
    });
  });

  it("should merge prefs when updating same column", async () => {
    mockedGetFromStorage.mockReturnValue({
      col1: { width: 100 },
    });

    const { result } = renderHook(() => useTableColumnPrefs("table1"));

    await waitFor(() => {
      expect(result.current.getColumnPrefs("col1")).toEqual({ width: 100 });
    });

    await act(() => {
      result.current.updateColumnPref("col1", "wrapped", true);
    });

    await waitFor(() => {
      expect(result.current.getColumnPrefs("col1")).toEqual({
        width: 100,
        wrapped: true,
      });
    });

    expect(mockedSaveToStorage).toHaveBeenCalledWith("table1:COLS_PREFS", {
      col1: { width: 100, wrapped: true },
    });
  });

  it("should register storage event listener", () => {
    mockedGetFromStorage.mockReturnValue({});

    renderHook(() => useTableColumnPrefs("table1"));

    expect(mockedUseEventListener).toHaveBeenCalledWith("storage", expect.any(Function));
  });

  it("should reload prefs when tableKey changes", async () => {
    mockedGetFromStorage
      .mockReturnValueOnce({ col1: { width: 100 } })
      .mockReturnValueOnce({ col2: { width: 200 } });

    const { result, rerender } = renderHook(
      ({ key }) => useTableColumnPrefs(key),
      { initialProps: { key: "table1" } },
    );

    await waitFor(() => {
      expect(result.current.getColumnPrefs("col1")).toEqual({ width: 100 });
    });

    rerender({ key: "table2" });

    await waitFor(() => {
      expect(result.current.getColumnPrefs("col2")).toEqual({ width: 200 });
    });

    expect(mockedGetFromStorage).toHaveBeenCalledWith("table1:COLS_PREFS");
    expect(mockedGetFromStorage).toHaveBeenCalledWith("table2:COLS_PREFS");
  });
});
