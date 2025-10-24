import type FileSystemFileHandle from "./FileSystemFileHandle";

// File type filter entry (WICG File System Access spec compatible).
interface FilePickerAcceptType {
  description?: string;
  accept: Record<string, string[] | undefined>;
}

// Options accepted by showSaveFilePicker (with internal flags supported by the polyfill).
export interface ShowSaveFilePickerOptions {
  excludeAcceptAllOption?: boolean;
  types?: FilePickerAcceptType[];
  suggestedName?: string;
  _name?: string; // Internal, deprecated in favor of `suggestedName`
  _preferPolyfill?: boolean; //  Force polyfill even if a native implementation exists
}

// Native showSaveFilePicker, if provided by the browser.
type ShowSaveFilePickerType = (options?: ShowSaveFilePickerOptions) => Promise<FileSystemFileHandle>
const native = (globalThis as { showSaveFilePicker?: ShowSaveFilePickerType }).showSaveFilePicker;

// Polyfill showSaveFilePicker.
// Uses the native API when available unless `_preferPolyfill` is set.
async function showSaveFilePicker(options: ShowSaveFilePickerOptions = {}): Promise<FileSystemFileHandle> {
  if (native && !options._preferPolyfill) {
    return native(options);
  }

  if (options._name) {
    console.warn("`_name` is deprecated; use `suggestedName` instead.");
    options.suggestedName = options._name;
  }

  const mod = (await import("./FileSystemFileHandle")) as typeof import("./FileSystemFileHandle");
  const { FileSystemFileHandle: FileSystemFileHandleClass } = mod;

  const down = (await import("./adapters/downloader")) as typeof import("./adapters/downloader");
  const { FileHandle } = down;

  return new FileSystemFileHandleClass(new FileHandle(options.suggestedName));
}

export default showSaveFilePicker;
export { showSaveFilePicker };
