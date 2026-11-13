import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { runWithTimeout } from "./utils";

describe("runWithTimeout", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("starts the deadline only when the request starts", async () => {
    let start!: () => void;
    let finish!: (value: string) => void;
    const abort = vi.fn(() => finish("aborted"));
    const request = runWithTimeout({
      run: startTimeout => { start = startTimeout; return new Promise<string>(resolve => { finish = resolve; }); },
      abort, isCurrent: () => true, timeoutMs: 3000,
    });
    await vi.advanceTimersByTimeAsync(10000);
    expect(abort).not.toHaveBeenCalled();
    start();
    await vi.advanceTimersByTimeAsync(2999);
    expect(abort).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(await request).toEqual({ result: "aborted", didTimeout: true });
    expect(vi.getTimerCount()).toBe(0);
  });

  it("does not abort a newer request when the old deadline expires", async () => {
    let current = true;
    let finish!: (value: string) => void;
    const abort = vi.fn();
    const request = runWithTimeout({
      run: start => { start(); return new Promise<string>(resolve => { finish = resolve; }); },
      abort, isCurrent: () => current, timeoutMs: 3000,
    });
    current = false;
    await vi.advanceTimersByTimeAsync(3000);
    expect(abort).not.toHaveBeenCalled();
    finish("aborted");
    expect(await request).toEqual({ result: "aborted", didTimeout: false });
  });

  it.each(["success", "aborted", "error"])("clears the deadline after %s", async result => {
    const abort = vi.fn();
    expect(await runWithTimeout({
      run: async start => { start(); return result; },
      abort, isCurrent: () => true, timeoutMs: 3000,
    })).toEqual({ result, didTimeout: false });
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(3000);
    expect(abort).not.toHaveBeenCalled();
  });

  it("clears the deadline when the request throws", async () => {
    await expect(runWithTimeout({
      run: async start => { start(); throw new Error("failed"); },
      abort: vi.fn(), isCurrent: () => true, timeoutMs: 3000,
    })).rejects.toThrow("failed");
    expect(vi.getTimerCount()).toBe(0);
  });
});
