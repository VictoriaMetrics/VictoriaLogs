import { describe, it, expect, beforeEach, vi } from "vitest";
import { renderHook, act } from "@testing-library/preact";
import { useLimitGuard } from "./useLimitGuard";
import type { BeforeFetchResult } from "../hooks/useFetchLogs";
import {
  LOGS_CONFIRM_THRESHOLD as THRESHOLD,
  LOGS_MAX_LIMIT as MAX,
  LOGS_LIMIT_WARN_DISMISSED_KEY as WARN_KEY,
} from "../../../constants/logs";

const makeBody = (n: number): URLSearchParams => {
  const p = new URLSearchParams();
  p.set("limit", String(n));
  return p;
};

describe("useLimitGuard (modal / proceed logic with real constants)", () => {
  beforeEach(() => {
    sessionStorage.clear();
    vi.restoreAllMocks();
  });

  it("proceeds without modal when 0 < limit <= THRESHOLD", async () => {
    const setLimit = vi.fn<(value: number) => void>();
    const { result } = renderHook(() => useLimitGuard({ setLimit }));

    const safe = Math.max(1, THRESHOLD); // covers THRESHOLD===0 edge
    const out = await result.current.beforeFetch(makeBody(safe));
    expect(out).toEqual({ action: "proceed" });
    expect(result.current.modalProps.isOpen).toBe(false);
    expect(setLimit).not.toHaveBeenCalled();
  });

  it("opens modal when limit > THRESHOLD", async () => {
    const setLimit = vi.fn<(value: number) => void>();
    const { result } = renderHook(() => useLimitGuard({ setLimit }));

    const soft = THRESHOLD + 1;
    const p = result.current.beforeFetch(makeBody(soft));
    expect(result.current.modalProps.isOpen).toBe(true);

    await act(async () => {
      result.current.modalProps.onCancel();
      await p;
    });

    expect(setLimit).not.toHaveBeenCalled();
  });

  it("opens modal when limit === 0", async () => {
    const setLimit = vi.fn<(value: number) => void>();
    const { result } = renderHook(() => useLimitGuard({ setLimit }));

    const p = result.current.beforeFetch(makeBody(0));
    expect(result.current.modalProps.isOpen).toBe(true);

    await act(async () => {
      result.current.modalProps.onCancel();
      await p;
    });
  });

  it("when modal is open and value is 0 -> it must NOT resolve to 'proceed' (must modify or abort)", async () => {
    const setLimit = vi.fn<(value: number) => void>();
    const { result } = renderHook(() => useLimitGuard({ setLimit }));

    const pending = result.current.beforeFetch(makeBody(0));
    let resolved: BeforeFetchResult | undefined;

    await act(async () => {
      result.current.modalProps.onConfirm();
      resolved = await pending;
    });

    expect(resolved).toBeDefined();
    expect(resolved!.action).not.toBe("proceed");
    if (resolved!.action === "modify") {
      expect(resolved!.body.get("limit")).toBe("0");
      expect(setLimit).toHaveBeenCalledWith(0);
    }
  });

  it("when modal is open and value > MAX -> it must NOT resolve to 'proceed' (must modify or abort)", async () => {
    const setLimit = vi.fn<(value: number) => void>();
    const { result } = renderHook(() => useLimitGuard({ setLimit }));

    const overMax = MAX + 123;
    const pending = result.current.beforeFetch(makeBody(overMax));
    expect(result.current.modalProps.initialLimit).toBe(overMax);

    // still beyond MAX; confirm should clamp to MAX
    act(() => {
      result.current.modalProps.setLimitDraft(MAX + 1);
    });

    let resolved: BeforeFetchResult | undefined;
    await act(async () => {
      result.current.modalProps.onConfirm();
      resolved = await pending;
    });

    expect(resolved).toBeDefined();
    expect(resolved!.action).not.toBe("proceed");
    if (resolved!.action === "modify") {
      expect(resolved!.body.get("limit")).toBe(String(MAX));
      expect(setLimit).toHaveBeenCalledWith(MAX);
    }
  });

  it("when modal is open and THRESHOLD < value <= MAX -> user may modify OR cancel", async () => {
    const setLimit = vi.fn<(value: number) => void>();
    const { result, rerender } = renderHook(() => useLimitGuard({ setLimit }));

    const softInRange = Math.min(MAX, THRESHOLD + 1);

    // A) modify
    let p = result.current.beforeFetch(makeBody(softInRange));
    const edited = Math.min(MAX, softInRange + 50);
    act(() => {
      result.current.modalProps.setLimitDraft(edited);
    });

    let resolvedA: BeforeFetchResult | undefined;
    await act(async () => {
      result.current.modalProps.onConfirm();
      resolvedA = await p;
    });
    expect(resolvedA!.action).toBe("modify");
    if (resolvedA!.action === "modify") {
      expect(resolvedA!.body.get("limit")).toBe(String(edited));
    }
    expect(setLimit).toHaveBeenLastCalledWith(edited);

    // B) cancel
    rerender();
    p = result.current.beforeFetch(makeBody(softInRange));
    let resolvedB: BeforeFetchResult | undefined;
    await act(async () => {
      result.current.modalProps.onCancel();
      resolvedB = await p;
    });
    expect(resolvedB).toEqual({ action: "abort" });
  });

  it("session suppression prevents soft-confirm modal", async () => {
    sessionStorage.setItem(WARN_KEY, "true");
    const setLimit = vi.fn<(value: number) => void>();
    const { result } = renderHook(() => useLimitGuard({ setLimit }));

    const soft = THRESHOLD + 1;
    const out = await result.current.beforeFetch(makeBody(soft));
    expect(out).toEqual({ action: "proceed" });
    expect(result.current.modalProps.isOpen).toBe(false);
  });

  it("deduplicates a pending promise while modal is open", async () => {
    const setLimit = vi.fn<(value: number) => void>();
    const { result } = renderHook(() => useLimitGuard({ setLimit }));

    const soft = THRESHOLD + 1;
    const p1 = result.current.beforeFetch(makeBody(soft));
    const p2 = result.current.beforeFetch(makeBody(soft + 10));
    expect(p1).toBe(p2);

    await act(async () => {
      result.current.modalProps.onCancel();
      await p1;
    });

    const p3 = result.current.beforeFetch(makeBody(soft));
    expect(p3).not.toBe(p1);

    await act(async () => {
      result.current.modalProps.onCancel();
      await p3;
    });
  });

  it("cleanup resolves pending promise with abort on unmount", async () => {
    const setLimit = vi.fn<(value: number) => void>();
    const { result, unmount } = renderHook(() => useLimitGuard({ setLimit }));

    const soft = THRESHOLD + 1;
    const pending = result.current.beforeFetch(makeBody(soft));
    let resolved: BeforeFetchResult | undefined;

    await act(async () => {
      unmount();
      resolved = await pending;
    });

    expect(resolved).toEqual({ action: "abort" });
    expect(setLimit).not.toHaveBeenCalled();
  });
});
