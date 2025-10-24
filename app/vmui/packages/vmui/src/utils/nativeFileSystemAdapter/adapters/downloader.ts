import { errors } from "../errors";
import config from "../config";
import { encodeRFC5987, isUint8Array } from "../utils";
import { FileSystemHandleAdapter } from "../FileSystemHandle";

type HeaderMap = Record<string, string>;

const {
  WritableStream: WritableStreamCtor,
  TransformStream: TransformStreamCtor,
  DOMException: DOMExceptionCtor,
  Blob: BlobCtor
} = config as {
  ReadableStream: typeof ReadableStream;
  WritableStream: typeof WritableStream;
  TransformStream: typeof TransformStream;
  DOMException: typeof DOMException;
  Blob: typeof Blob;
  File: typeof File;
};

const { GONE } = errors;

// Don't match newer versions of Safari, but that's okay.
// Use a safe stringify of the constructor function to avoid `any`.
const isOldSafari: boolean = /constructor/i.test(
  Function.prototype.toString.call(HTMLElement)
);

export interface CreateWritableOptions {
  // Optional total size (bytes) to send as Content-Length.
  size?: number;
}

export class FileHandle implements FileSystemHandleAdapter {
  public readonly name: string;
  public readonly kind = "file" as const;
  public readonly writable = true;

  constructor(name: string = "unknown") {
    this.name = name;
  }

  async getFile(): Promise<File> {
    // This fallback handle never resolves to a File.
    throw new DOMExceptionCtor(...GONE);
  }

  async isSameEntry(other: unknown): Promise<boolean> {
    return this === other;
  }

  async remove(_opts?: { recursive?: boolean }): Promise<void> {
    // no-op for downloader backend
  }

  async createWritable(options: CreateWritableOptions = {}): Promise<WritableStreamDefaultWriter<BlobPart>> {
    const sw: ServiceWorkerRegistration | undefined =
      await navigator.serviceWorker?.getRegistration();

    const link = document.createElement("a");
    const ts = new TransformStreamCtor<BlobPart, BlobPart>();
    const sink: WritableStream<BlobPart> = ts.writable;

    link.download = this.name;

    if (isOldSafari || !sw) {
      // Fallback: collect chunks and trigger a blob download.
      let chunks: Blob[] = [];
      ts.readable.pipeTo(
        new WritableStream<BlobPart>({
          write(chunk: BlobPart) {
            chunks.push(new BlobCtor([chunk]));
          },
          close() {
            const blob = new BlobCtor(chunks, { type: "application/octet-stream; charset=utf-8" });
            chunks = [];
            link.href = URL.createObjectURL(blob);
            link.click();
            window.setTimeout(() => URL.revokeObjectURL(link.href), 10_000);
          }
        })
      ).catch(() => {
      });
    } else {
      const { writable, readablePort } = new RemoteWritableStream(WritableStreamCtor);

      // Make filename RFC5987-compatible
      const fileName = encodeRFC5987(this.name);

      const headers: HeaderMap = {
        "content-disposition": "attachment; filename*=UTF-8''" + fileName,
        "content-type": "application/octet-stream; charset=utf-8",
        ...(options.size !== undefined ? { "content-length": String(options.size) } : {})
      };

      // Periodic keep-alive pings while streaming (one-shot here; clearing with clearTimeout).
      const keepAlive: number = window.setTimeout(() => sw.active?.postMessage(0), 10_000);

      ts.readable
        .pipeThrough(
          new TransformStreamCtor<BlobPart, Uint8Array>({
            async transform(chunk: BlobPart, ctrl: TransformStreamDefaultController<Uint8Array>) {
              if (isUint8Array(chunk)) {
                ctrl.enqueue(chunk);
                return;
              }
              const response = new Response(chunk);
              const body = response.body;
              if (!body) return;

              const reader = body.getReader();
              const pump = async (): Promise<void> => {
                const { done, value } = await reader.read();
                if (done) return;
                ctrl.enqueue(value);
                return pump();
              };
              await pump();
            }
          })
        )
        .pipeTo(writable)
        .finally(() => {
          clearTimeout(keepAlive);
        });

      // Transfer the stream to the service worker
      sw.active?.postMessage(
        {
          url: sw.scope + fileName,
          headers,
          readablePort
        },
        [readablePort]
      );

      // Trigger the download with a hidden iframe
      const iframe = document.createElement("iframe");
      iframe.hidden = true;
      iframe.src = sw.scope + fileName;
      document.body.appendChild(iframe);
    }

    return sink.getWriter();
  }
}

// Want to remove this postMessage hack, tell them you want transferable streams:
// https://bugs.webkit.org/show_bug.cgi?id=215485

const WRITE = 0 as const;
const PULL = 0 as const;
const ERROR = 1 as const;
const ABORT = 1 as const;
const CLOSE = 2 as const;

type InboundMessage =
  | { type: typeof PULL }
  | { type: typeof ERROR; reason: unknown };

type OutboundMessage =
  | { type: typeof WRITE; chunk: Uint8Array }
  | { type: typeof ABORT; reason?: unknown }
  | { type: typeof CLOSE };

class MessagePortSink implements UnderlyingSink<Uint8Array> {
  private _port: MessagePort;
  private _controller!: WritableStreamDefaultController;
  private _readyPromise!: Promise<void>;
  private _readyResolve!: () => void;
  private _readyReject!: (reason?: unknown) => void;
  private _readyPending = false;

  constructor(port: MessagePort) {
    this._port = port;
    this._port.onmessage = (event: MessageEvent<InboundMessage>) => this._onMessage(event.data);
    this._resetReady();
  }

  start(controller: WritableStreamDefaultController): void | Promise<void> {
    this._controller = controller;
    // Apply initial backpressure
    return this._readyPromise;
  }

  write(chunk: Uint8Array): void | Promise<void> {
    const message: OutboundMessage = { type: WRITE, chunk };

    // Send chunk (transfer ArrayBuffer to avoid copies)
    this._port.postMessage(message, [chunk.buffer]);

    // Assume backpressure after every write, until sender pulls
    this._resetReady();

    // Apply backpressure
    return this._readyPromise;
  }

  close(): void | Promise<void> {
    const message: OutboundMessage = { type: CLOSE };
    this._port.postMessage(message);
    this._port.close();
  }

  abort(reason?: unknown): void | Promise<void> {
    const message: OutboundMessage = { type: ABORT, reason };
    this._port.postMessage(message);
    this._port.close();
  }

  private _onMessage(message: InboundMessage): void {
    if (message.type === PULL) this._resolveReady();
    if (message.type === ERROR) this._onError(message.reason);
  }

  private _onError(reason: unknown): void {
    this._controller.error(reason);
    this._rejectReady(reason);
    this._port.close();
  }

  private _resetReady(): void {
    this._readyPromise = new Promise<void>((resolve, reject) => {
      this._readyResolve = resolve;
      this._readyReject = reject;
    });
    this._readyPending = true;
  }

  private _resolveReady(): void {
    this._readyResolve();
    this._readyPending = false;
  }

  private _rejectReady(reason?: unknown): void {
    if (!this._readyPending) this._resetReady();
    // Swallow unhandled rejection to avoid noisy logs
    this._readyPromise.catch(() => {
    });
    this._readyReject(reason);
    this._readyPending = false;
  }
}

class RemoteWritableStream {
  public readonly readablePort: MessagePort;
  public readonly writable: WritableStream<Uint8Array>;

  constructor(
    WritableStreamClass: new (
      underlyingSink?: UnderlyingSink<Uint8Array>,
      strategy?: QueuingStrategy<Uint8Array>
    ) => WritableStream<Uint8Array>
  ) {
    const channel = new MessageChannel();
    this.readablePort = channel.port1;
    this.writable = new WritableStreamClass(new MessagePortSink(channel.port2));
  }
}
