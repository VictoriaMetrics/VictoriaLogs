type FSFHProtoShape = { prototype: { createWritable?: unknown } };

const FSFH = (globalThis as { FileSystemFileHandle?: FSFHProtoShape }).FileSystemFileHandle;

if (FSFH && !("createWritable" in FSFH.prototype)) {
  // Keep a parent-dir reference for each file handle so we can resolve an OPFS path in the worker
  const wm = new WeakMap<FileSystemFileHandle, FileSystemDirectoryHandle>();

  // Track the parent directory for each handle so we can resolve its path later
  const orig = FileSystemDirectoryHandle.prototype.getFileHandle as (
    this: FileSystemDirectoryHandle,
    name: string,
    options?: { create?: boolean }
  ) => Promise<FileSystemFileHandle>;

  FileSystemDirectoryHandle.prototype.getFileHandle = (async function getFileHandle(
    this: FileSystemDirectoryHandle,
    ...args: Parameters<typeof orig>
  ): ReturnType<typeof orig> {
    const handle = await orig.call(this, ...args);
    wm.set(handle, this);
    return handle;
  }) as typeof orig;
}
