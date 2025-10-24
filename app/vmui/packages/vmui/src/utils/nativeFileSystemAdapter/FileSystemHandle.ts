const kAdapter = Symbol("adapter");

export type FileSystemPermissionMode = "read" | "readwrite";

export interface FileSystemHandlePermissionDescriptor {
  mode?: FileSystemPermissionMode;
}

type FileSystemHandleKind = "file" | "directory";

// Adapter contract backing a FileSystemHandle instance.
interface FileSystemHandleAdapter {
  name: string;
  kind: FileSystemHandleKind;
  writable?: unknown; // Presence indicates write capability.

  queryPermission?(descriptor: { mode: FileSystemPermissionMode }): PermissionState | Promise<PermissionState>;

  requestPermission?(descriptor: { mode: FileSystemPermissionMode }): PermissionState | Promise<PermissionState>;

  remove(options?: { recursive?: boolean }): void | Promise<void>;

  isSameEntry(other: FileSystemHandleAdapter): boolean | Promise<boolean>;
}

class FileSystemHandle {
  public readonly name: string;
  public readonly kind: FileSystemHandleKind;

  // Backing adapter
  private [kAdapter]: FileSystemHandleAdapter;

  constructor(adapter: FileSystemHandleAdapter) {
    this.kind = adapter.kind;
    this.name = adapter.name;
    this[kAdapter] = adapter;
  }

  async queryPermission(descriptor: FileSystemHandlePermissionDescriptor = {}): Promise<PermissionState> {
    const { mode = "read" } = descriptor;
    const handle = this[kAdapter];

    if (handle.queryPermission) {
      return handle.queryPermission({ mode });
    }

    if (mode === "read") {
      return "granted";
    } else if (mode === "readwrite") {
      return handle.writable ? "granted" : "denied";
    }

    throw new TypeError(`Mode ${mode} must be 'read' or 'readwrite'`);
  }

  async requestPermission({ mode = "read" }: FileSystemHandlePermissionDescriptor = {}): Promise<PermissionState> {
    const handle = this[kAdapter];

    if (handle.requestPermission) {
      return handle.requestPermission({ mode });
    }

    if (mode === "read") {
      return "granted";
    } else if (mode === "readwrite") {
      return handle.writable ? "granted" : "denied";
    }

    throw new TypeError(`Mode ${mode} must be 'read' or 'readwrite'`);
  }

  // Attempts to remove the entry represented by handle from the underlying file system.
  async remove(options: { recursive?: boolean } = {}): Promise<void> {
    await this[kAdapter].remove(options);
  }

  async isSameEntry(other: FileSystemHandle): Promise<boolean> {
    if (this === other) return true;
    if (!other || typeof other !== "object" || this.kind !== other.kind) return false;
    // Accessible here because it's the same class scope.
    return this[kAdapter].isSameEntry(other[kAdapter]);
  }
}

Object.defineProperty(FileSystemHandle.prototype, Symbol.toStringTag, {
  value: "FileSystemHandle",
  writable: false,
  enumerable: false,
  configurable: true
});

// Safari doesn't support writable streams yet.
// If a global FileSystemHandle exists, ensure queryPermission is defined.
const GlobalFSH = (globalThis as unknown as {
  FileSystemHandle?: {
    prototype: {
      queryPermission?: (descriptor?: FileSystemHandlePermissionDescriptor) => PermissionState | Promise<PermissionState>;
    };
  };
}).FileSystemHandle;

if (GlobalFSH) {
  GlobalFSH.prototype.queryPermission ??= function (_descriptor?: FileSystemHandlePermissionDescriptor): PermissionState {
    return "granted";
  };
}

export default FileSystemHandle;
export { FileSystemHandle, type FileSystemHandleAdapter };
