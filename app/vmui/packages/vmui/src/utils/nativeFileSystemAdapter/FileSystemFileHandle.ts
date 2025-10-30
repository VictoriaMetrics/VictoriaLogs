import FileSystemHandle, { FileSystemHandleAdapter } from "./FileSystemHandle";
import FileSystemWritableFileStream from "./FileSystemWritableFileStream";
import "./createWritable";

// Options for createWritable: union of spec plus adapter's size hint
export interface FileSystemCreateWritableOptions {
  keepExistingData?: boolean;
  size?: number; // used by the downloader adapter for Content-Length
}

// This automatically resolves to UnderlyingWritableLike<...> after step (1)
type WritableCtorParam = ConstructorParameters<typeof FileSystemWritableFileStream>[0];

type FileHandleAdapter = FileSystemHandleAdapter & {
  createWritable(options?: FileSystemCreateWritableOptions): Promise<WritableCtorParam>;
  getFile(): Promise<File>;
};

// Internal adapter symbol
const kAdapter = Symbol("adapter");

class FileSystemFileHandle extends FileSystemHandle {
  // Backing adapter for this handle
  private [kAdapter]: FileHandleAdapter;

  constructor(adapter: FileHandleAdapter) {
    super(adapter);
    this[kAdapter] = adapter;
  }

  // Create a writable stream to this file.
  async createWritable(options: FileSystemCreateWritableOptions = {}): Promise<FileSystemWritableFileStream> {
    const impl = await this[kAdapter].createWritable(options);
    return new FileSystemWritableFileStream(impl);
  }

  // Get a snapshot File of the current contents.
  async getFile(): Promise<File> {
    return this[kAdapter].getFile();
  }
}

Object.defineProperty(FileSystemFileHandle.prototype, Symbol.toStringTag, {
  value: "FileSystemFileHandle",
  writable: false,
  enumerable: false,
  configurable: true
});

Object.defineProperties(FileSystemFileHandle.prototype, {
  createWritable: { enumerable: true },
  getFile: { enumerable: true }
});

export default FileSystemFileHandle;
export { FileSystemFileHandle };
