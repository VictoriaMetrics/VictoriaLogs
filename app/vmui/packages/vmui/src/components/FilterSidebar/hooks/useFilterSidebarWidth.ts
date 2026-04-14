import { RefObject, useEffect, useState } from "preact/compat";
import { getFromStorage, removeFromStorage, saveToStorage } from "../../../utils/storage";
import { Size, useResizeObserver } from "../../../hooks/useResizeObserver";
import useEventListener from "../../../hooks/useEventListener";

const WIDTH_STORAGE_KEY = "LOGS_FILTER_SIDEBAR_WIDTH";

export function useFilterSidebarWidth<T extends HTMLElement>(
  ref: RefObject<T>,
) {
  const [width, setWidth] = useState(0);

  const [size, setSize] = useState<Size>({
    width: ref?.current?.offsetWidth,
    height: ref?.current?.offsetHeight
  });
  useResizeObserver({ ref, onResize: setSize });

  const setStorageWidth = (next: number) => {
    setWidth(prev => (prev !== next ? next : prev));
    saveToStorage(WIDTH_STORAGE_KEY, String(next));
  };

  const clearStorageWidth = () => {
    setWidth(0);
    removeFromStorage([WIDTH_STORAGE_KEY]);
  };

  const getStorageWidth = () => {
    const raw = getFromStorage(WIDTH_STORAGE_KEY);
    const num = raw ? Number(raw) : 0;
    return Number.isFinite(num) ? num : 0;
  };

  const updateStateWidth = () => {
    setWidth(prev => {
      const next = getStorageWidth();
      return prev !== next ? next : prev;
    });
  };

  useEventListener("storage", updateStateWidth);
  useEffect(() => {
    updateStateWidth();
  }, []);

  return {
    size,
    width,
    setWidth: setStorageWidth,
    clearWidth: clearStorageWidth,
  };
}
