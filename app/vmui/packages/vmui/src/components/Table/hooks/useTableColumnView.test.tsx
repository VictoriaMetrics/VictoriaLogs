import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, renderHook, waitFor } from "@testing-library/preact";

import { useTableColumnView, type ViewColumnsAction } from "./useTableColumnView";

type RouterDomMock = {
  __setInitialSearchParams: (p: URLSearchParams) => void;
  __getLastSearchParams: () => URLSearchParams | null;
};

// -------------------- mocks --------------------

// in-memory storage per test
let storage = new Map<string, unknown>();

vi.mock("../../../utils/storage", async () => {
  return {
    getTableStorageKey: (tableKey: string, suffix: string) => `${tableKey}__${suffix}`,
    getFromStorage: (key: string) => storage.get(key),
    saveToStorage: (key: string, value: unknown) => storage.set(key, value),
    removeFromStorage: (keys: string[]) => keys.forEach(k => storage.delete(k)),
  };
});

vi.mock("../../../constants/logs", async () => {
  return {
    DEFAULT_COMMON_FIELDS: ["ts", "level", "message"],
    LOGS_URL_PARAMS: { COLUMNS: "columns" },
  };
});

// capture storage handler so tests can trigger it
let storageListener: (() => void) | null = null;

vi.mock("../../../hooks/useEventListener", async () => {
  return {
    default: (_eventName: string, handler: () => void) => {
      storageListener = handler;
    },
  };
});

// mock react-router-dom useSearchParams WITHOUT bringing the real router
let initialParams = new URLSearchParams();
let lastParams: URLSearchParams | null = null;

vi.mock("react-router-dom", async () => {
  const { useState } = await import("preact/compat");

  const useSearchParams = () => {
    const [params, setParams] = useState<URLSearchParams>(initialParams);
    lastParams = params;

    const setSearchParams = (updater: unknown) => {
      setParams((prev: URLSearchParams) => {
        const next = typeof updater === "function" ? updater(prev) : updater;
        return next instanceof URLSearchParams ? next : new URLSearchParams(next);
      });
    };

    return [params, setSearchParams] as const;
  };

  return {
    useSearchParams,
    // helpers for tests
    __setInitialSearchParams: (p: URLSearchParams) => {
      initialParams = p;
      lastParams = p;
    },
    __getLastSearchParams: () => lastParams,
  };
});

// typed access to helpers from the mocked module
const routerMock = (await import("react-router-dom")) as unknown as RouterDomMock;

const setUrlColumns = (keys?: string[]) => {
  const sp = new URLSearchParams();
  if (keys) sp.set("columns", JSON.stringify(keys));
  routerMock.__setInitialSearchParams(sp);
};

const getUrlColumnsRaw = () => {
  const sp: URLSearchParams | null = routerMock.__getLastSearchParams();
  return sp?.get("columns");
};

// -------------------- tests --------------------

describe("useTableColumnView", () => {
  beforeEach(() => {
    storage = new Map<string, unknown>();
    storageListener = null;
    setUrlColumns(undefined);
  });

  it("initializes with DEFAULT_COMMON_FIELDS when no URL and no storage and no primaryKeys", () => {
    const { result } = renderHook(() =>
      useTableColumnView("tableA", ["host", "ts", "level", "message"]),
    );
    expect(result.current.viewColumnKeys).toEqual(["ts", "level", "message"]);
  });

  it("adds primaryKeys to defaults only if they exist in availableColumnKeys", () => {
    const { result } = renderHook(() =>
      useTableColumnView(
        "tableA",
        ["host", "ts", "level", "message"], // host available
        ["host", "missing"], // only host should be added
      ),
    );
    expect(result.current.viewColumnKeys).toEqual(["ts", "level", "message", "host"]);
  });

  it("prefers URL over storage", async () => {
    storage.set("tableA__COLS_VIEW", ["ts"]);
    setUrlColumns(["level", "message"]);

    const { result } = renderHook(() =>
      useTableColumnView("tableA", ["host", "ts", "level", "message"]),
    );

    await waitFor(() => {
      expect(result.current.viewColumnKeys).toEqual(["level", "message"]);
    });
  });

  it("prefers storage when URL missing", async () => {
    storage.set("tableA__COLS_VIEW", ["level", "ts"]);
    setUrlColumns(undefined);

    const { result } = renderHook(() =>
      useTableColumnView("tableA", ["host", "ts", "level", "message"]),
    );

    await waitFor(() => {
      expect(result.current.viewColumnKeys).toEqual(["level", "ts"]);
    });
  });

  it("ignores invalid URL and falls back to storage/default", async () => {
    storage.set("tableA__COLS_VIEW", ["message"]);

    // invalid JSON in URL
    const sp = new URLSearchParams();
    sp.set("columns", "{not-json");
    routerMock.__setInitialSearchParams(sp);

    const { result } = renderHook(() =>
      useTableColumnView("tableA", ["host", "ts", "level", "message"]),
    );

    await waitFor(() => {
      expect(result.current.viewColumnKeys).toEqual(["message"]);
    });
  });

  it("dispatch replace updates state and persists to storage and URL", async () => {
    const { result } = renderHook(() =>
      useTableColumnView("tableA", ["host", "ts", "level", "message"]),
    );

    await act(() => {
      const action: ViewColumnsAction = { type: "replace", columnKeys: ["level", "host"] };
      result.current.dispatchViewColumns(action);
    });

    await waitFor(() => {
      expect(result.current.viewColumnKeys).toEqual(["level", "host"]);
    });

    expect(storage.get("tableA__COLS_VIEW")).toEqual(["level", "host"]);
    expect(getUrlColumnsRaw()).toBe(JSON.stringify(["level", "host"]));
  });

  it("dispatch reset sets default keys and clears storage and URL", async () => {
    storage.set("tableA__COLS_VIEW", ["level"]);
    setUrlColumns(["level"]);

    const { result } = renderHook(() =>
      useTableColumnView(
        "tableA",
        ["host", "ts", "level", "message"],
        ["host"], // host is part of default view
      ),
    );

    await waitFor(() => {
      expect(result.current.viewColumnKeys).toEqual(["level"]);
    });

    await act(() => {
      const action: ViewColumnsAction = { type: "reset" };
      result.current.dispatchViewColumns(action);
    });

    await waitFor(() => {
      expect(result.current.viewColumnKeys).toEqual(["ts", "level", "message", "host"]);
    });

    expect(storage.get("tableA__COLS_VIEW")).toBeUndefined();
    expect(getUrlColumnsRaw()).toBeNull();
  });

  it("dispatch toggle adds/removes and persists; removal back to default clears storage and URL", async () => {
    const { result } = renderHook(() =>
      useTableColumnView("tableA", ["host", "ts", "level", "message"]),
    );

    // default: ["ts","level","message"]
    await act(() => {
      result.current.dispatchViewColumns({ type: "toggle", columnKey: "host" });
    });

    await waitFor(() => {
      expect(result.current.viewColumnKeys).toEqual(["ts", "level", "message", "host"]);
    });

    expect(storage.get("tableA__COLS_VIEW")).toEqual(["ts", "level", "message", "host"]);
    expect(getUrlColumnsRaw()).toBe(JSON.stringify(["ts", "level", "message", "host"]));

    await act(() => {
      result.current.dispatchViewColumns({ type: "toggle", columnKey: "host" });
    });

    await waitFor(() => {
      expect(result.current.viewColumnKeys).toEqual(["ts", "level", "message"]);
    });

    // back to default => cleared
    expect(storage.get("tableA__COLS_VIEW")).toBeUndefined();
    expect(getUrlColumnsRaw()).toBeNull();
  });

  it("syncs on storage event", async () => {
    const { result } = renderHook(() =>
      useTableColumnView("tableA", ["host", "ts", "level", "message"]),
    );

    await waitFor(() => {
      expect(result.current.viewColumnKeys).toEqual(["ts", "level", "message"]);
    });

    // simulate external update
    storage.set("tableA__COLS_VIEW", ["message", "host"]);

    await act(() => {
      storageListener?.();
    });

    await waitFor(() => {
      expect(result.current.viewColumnKeys).toEqual(["message", "host"]);
    });
  });
});
