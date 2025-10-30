import config from "./config";

export type FileSystemWriteChunkType = BufferSource | Blob | string;
export type FileSystemWriteParams =
  | { type: "write"; position?: number; data: FileSystemWriteChunkType }
  | { type: "seek"; position: number }
  | { type: "truncate"; size: number };

type Chunk = FileSystemWriteChunkType | FileSystemWriteParams;

type WritableStreamCtor<T> = new (
  underlyingSink?: UnderlyingSink<T>,
  strategy?: QueuingStrategy<T>
) => WritableStream<T>;

const WritableStreamBase =
  (config.WritableStream as unknown as WritableStreamCtor<Chunk>) ??
  ((globalThis as unknown as { WritableStream: WritableStreamCtor<Chunk> }).WritableStream);

// Accept either an UnderlyingSink or a WritableStreamDefaultWriter
export type UnderlyingWritableLike<T> =
  | UnderlyingSink<T>
  | WritableStreamDefaultWriter<unknown>;

class FileSystemWritableFileStream extends WritableStreamBase {
  private _closed = false;

  constructor(writer: UnderlyingWritableLike<Chunk>) {
    // Cast through unknown to satisfy the ctor without using `any`
    super(writer as unknown as UnderlyingSink<unknown>);

    // Safari native-class inheritance hack
    Object.setPrototypeOf(this, FileSystemWritableFileStream.prototype);
  }

  async close(): Promise<void> {
    this._closed = true;
    const w = this.getWriter();
    const p = w.close();
    w.releaseLock();
    return p;
  }

  // Seek to an absolute byte offset.
  seek(position: number): Promise<void> {
    return this.write({ type: "seek", position });
  }

  // Truncate to the given size (bytes)
  truncate(size: number): Promise<void> {
    return this.write({ type: "truncate", size });
  }

  // Write raw data or a write/seek/truncate command object
  write(data: Chunk): Promise<void> {
    if (this._closed) {
      return Promise.reject(new TypeError("Cannot write to a CLOSED writable stream"));
    }
    const writer = this.getWriter();
    const result = writer.write(data);
    writer.releaseLock();
    return result;
  }
}

Object.defineProperty(FileSystemWritableFileStream.prototype, Symbol.toStringTag, {
  value: "FileSystemWritableFileStream",
  writable: false,
  enumerable: false,
  configurable: true
});

Object.defineProperties(FileSystemWritableFileStream.prototype, {
  close: { enumerable: true },
  seek: { enumerable: true },
  truncate: { enumerable: true },
  write: { enumerable: true }
});

// Expose globally for old Safari if needed
{
  const g = globalThis as unknown as {
    FileSystemFileHandle?: { prototype: { createWritable?: unknown } };
    FileSystemWritableFileStream?: typeof FileSystemWritableFileStream;
  };
  if (g.FileSystemFileHandle &&
    !g.FileSystemFileHandle.prototype.createWritable &&
    !g.FileSystemWritableFileStream) {
    g.FileSystemWritableFileStream = FileSystemWritableFileStream;
  }
}

export default FileSystemWritableFileStream;
export { FileSystemWritableFileStream };
