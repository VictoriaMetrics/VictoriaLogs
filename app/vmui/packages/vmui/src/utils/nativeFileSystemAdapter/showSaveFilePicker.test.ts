import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import config from "./config";

async function ensureStreams(): Promise<void> {
  // Get streams from globals or Node's stream/web (fallback).
  let RS: typeof ReadableStream | undefined = globalThis.ReadableStream;
  let WS: typeof WritableStream | undefined = globalThis.WritableStream;
  let TS: typeof TransformStream | undefined = globalThis.TransformStream;

  if (!RS || !WS || !TS) {
    // Node built-in, no install needed. Types differ from DOM — safe for tests.
    const web = (await import("stream/web")) as unknown as {
      ReadableStream: typeof ReadableStream;
      WritableStream: typeof WritableStream;
      TransformStream: typeof TransformStream;
    };
    RS ??= web.ReadableStream;
    WS ??= web.WritableStream;
    TS ??= web.TransformStream;
  }

  // Feed constructors to config only (no touching globalThis).
  const cfg = config as unknown as Record<string, unknown>;
  cfg.ReadableStream = RS;
  cfg.WritableStream = WS;
  cfg.TransformStream = TS;
  cfg.DOMException = globalThis.DOMException;
  cfg.Blob = globalThis.Blob;
  cfg.File = globalThis.File;
}

function stubURL(create: (b: Blob) => string, revoke: (url: string) => void): void {
  const u = globalThis.URL as unknown as Record<string, unknown>;
  u.createObjectURL = create;
  u.revokeObjectURL = revoke;
}

function setServiceWorker(sw: { getRegistration: () => Promise<unknown> } | undefined): void {
  Object.defineProperty(globalThis.navigator, "serviceWorker", { configurable: true, value: sw });
}

// Minimal MessageChannel for RemoteWritableStream
class FakePort {
  onmessage: ((e: MessageEvent<unknown>) => void) | null = null;
  postMessage = vi.fn<(msg: unknown, transfer?: unknown[]) => void>();
  close = vi.fn<() => void>();
}

class FakeMessageChannel {
  port1 = new FakePort() as unknown as MessagePort;
  port2 = new FakePort() as unknown as MessagePort;
}

describe("showSaveFilePicker polyfill", () => {
  beforeEach(async () => {
    vi.restoreAllMocks();
    vi.resetModules();
    await ensureStreams();

    (globalThis as unknown as { showSaveFilePicker?: unknown }).showSaveFilePicker = undefined;
    setServiceWorker(undefined);

    // Prevent "old Safari" branch in tests.
    Object.defineProperty(globalThis, "HTMLElement", {
      configurable: true, value: function HTMLFake() {
      }
    });

    document.body.innerHTML = "";
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("uses native API when present", async () => {
    const nativeResult = {} as unknown;
    const native = vi.fn<(o?: unknown) => Promise<unknown>>().mockResolvedValue(nativeResult);
    (globalThis as unknown as { showSaveFilePicker?: typeof native }).showSaveFilePicker = native;

    const { showSaveFilePicker } = await import("./showSaveFilePicker");
    const res = await showSaveFilePicker({ suggestedName: "native.txt" });

    expect(native).toHaveBeenCalledTimes(1);
    expect(res).toBe(nativeResult);
  });

  it("falls back to Blob download when no Service Worker", async () => {
    const createObjectURL = vi.fn<(b: Blob) => string>().mockReturnValue("blob:url");
    const revokeObjectURL = vi.fn<(url: string) => void>();
    stubURL(createObjectURL, revokeObjectURL);

    const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => void 0);

    const { showSaveFilePicker } = await import("./showSaveFilePicker");
    const handle = await showSaveFilePicker({ suggestedName: "log.txt", _preferPolyfill: true });

    const writer = await handle.createWritable();
    await writer.write("hello");
    await writer.close();

    expect(createObjectURL).toHaveBeenCalledTimes(1);
    expect(clickSpy).toHaveBeenCalledTimes(1);

    vi.advanceTimersByTime(10_000);
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:url");
  });

  it("streams via Service Worker and posts payload with headers", async () => {
    (globalThis as unknown as { MessageChannel: unknown }).MessageChannel = FakeMessageChannel;

    const swPost = vi.fn<(msg: unknown, transfer?: unknown[]) => void>();
    const swReg = { scope: "/sw/", active: { postMessage: swPost } } as const;
    setServiceWorker({ getRegistration: () => Promise.resolve(swReg) });

    const { showSaveFilePicker } = await import("./showSaveFilePicker");
    const handle = await showSaveFilePicker({ suggestedName: "file (1).txt", _preferPolyfill: true });

    await handle.createWritable({ size: 42 });

    const call = swPost.mock.calls.at(-1);
    expect(call).toBeDefined();

    const transfer = call![1] as unknown[] | undefined;
    expect(Array.isArray(transfer)).toBe(true);
    expect(transfer!.length).toBe(1); // readablePort transferred

    const expectedName = encodeURIComponent("file (1).txt").replace(
      /[!'()*]/g,
      (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`
    );

    const payload = call![0] as { url: string; headers: Record<string, string> };
    expect(payload.url).toBe("/sw/" + expectedName);
    expect(payload.headers["content-disposition"]).toContain(`filename*=UTF-8''${expectedName}`);
    expect(payload.headers["content-type"]).toMatch(/octet-stream/i);
    expect(payload.headers["content-length"]).toBe("42");
  });
});
