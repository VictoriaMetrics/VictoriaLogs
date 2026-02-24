import { useEffect, useMemo, useState } from "preact/compat";
import { getFromStorage, removeFromStorage, getTableStorageKey, saveToStorage } from "../../../utils/storage";
import { DEFAULT_COMMON_FIELDS, LOGS_URL_PARAMS } from "../../../constants/logs";
import { useSearchParams } from "react-router-dom";
import { arrayEquals } from "../../../utils/array";
import useEventListener from "../../../hooks/useEventListener";

const COLUMNS_URL_PARAM_KEY = LOGS_URL_PARAMS.COLUMNS;

export type ViewColumnsAction =
  | { type: "set"; columnKey: string; visible: boolean }
  | { type: "toggle"; columnKey: string }
  | { type: "replace"; columnKeys: string[] }
  | { type: "reset" };

export const useTableColumnView = (
  tableKey: string,
  availableColumnKeys: string[],
  primaryKeys?: string[],
) => {
  const [searchParams, setSearchParams] = useSearchParams();

  const storageKey = getTableStorageKey(tableKey, "COLS_VIEW");

  const primaryKeysSignature = primaryKeys?.join("\u001F") ?? "";

  const defaultKeys = useMemo(() => {
    const result = new Set<string>(DEFAULT_COMMON_FIELDS);

    primaryKeys?.forEach(col => {
      if (availableColumnKeys.includes(col)) result.add(col);
    });

    return [...result];
  }, [primaryKeysSignature, availableColumnKeys]);

  const [viewColumnKeys, setViewColumnKeys] = useState<string[]>(defaultKeys);

  const clearKeys = () => {
    setSearchParams(prev => {
      const nextParams = new URLSearchParams(prev);
      nextParams.delete(COLUMNS_URL_PARAM_KEY);
      return nextParams;
    });

    removeFromStorage([storageKey]);
  };

  const getKeysFromStorage = () => {
    const stored = getFromStorage(storageKey);
    if (!stored || !Array.isArray(stored)) return;
    return stored as string[];
  };

  const getKeysFromUrl = () => {
    const raw = searchParams.get(COLUMNS_URL_PARAM_KEY);
    if (!raw) return;
    try {
      const parsedValue = JSON.parse(raw);
      if (!Array.isArray(parsedValue)) return;
      return parsedValue.filter((v: unknown): v is string => typeof v === "string" && v.length > 0);
    } catch (e) {
      return;
    }
  };

  const syncKeys = () => {
    const storageKeys = getKeysFromStorage();
    const urlKeys = getKeysFromUrl();
    const nextKeys = urlKeys || storageKeys || defaultKeys;
    setViewColumnKeys(prevKeys => (arrayEquals(prevKeys, nextKeys) ? prevKeys : nextKeys));
  };

  const persistKeys = (next: string[]) => {
    const isDefaultView = arrayEquals(next, defaultKeys);
    if (isDefaultView) {
      clearKeys();
    } else {
      saveToStorage(storageKey, next);
      setSearchParams(prev => {
        const nextParams = new URLSearchParams(prev);
        nextParams.set(COLUMNS_URL_PARAM_KEY, JSON.stringify(next));
        return nextParams;
      });
    }
  };

  const updateKeys = (updater: (prev: string[]) => string[]) => {
    setViewColumnKeys(prev => {
      const next = updater(prev);
      if (!arrayEquals(prev, next)) persistKeys(next);
      return next;
    });
  };

  const dispatchViewColumns = (action: ViewColumnsAction) => {
    updateKeys(prev => {
      switch (action.type) {
        case "replace":
          return action.columnKeys;

        case "reset":
          return defaultKeys;

        case "toggle": {
          const has = prev.includes(action.columnKey);
          return has ? prev.filter(c => c !== action.columnKey) : [...prev, action.columnKey];
        }

        case "set": {
          const has = prev.includes(action.columnKey);
          if (action.visible) return has ? prev : [...prev, action.columnKey];
          return has ? prev.filter(c => c !== action.columnKey) : prev;
        }
      }
    });
  };

  const urlRaw = searchParams.get(COLUMNS_URL_PARAM_KEY);
  useEffect(syncKeys, [storageKey, urlRaw, defaultKeys]);
  useEventListener("storage", syncKeys);

  return {
    viewColumnKeys,
    dispatchViewColumns,
  };
};
