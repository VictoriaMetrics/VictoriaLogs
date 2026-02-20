import { useEffect, useState, useCallback } from "preact/compat";
import { getFromStorage, saveToStorage, getTableStorageKey } from "../../../utils/storage";
import useEventListener from "../../../hooks/useEventListener";

export type ColumnPrefs = {
  width?: number;
  wrapped?: boolean;
}

type ColumnPrefsMap = Map<string, ColumnPrefs>;

export const useTableColumnPrefs = (tableKey: string) => {
  const storageKey = getTableStorageKey(tableKey, "COLS_PREFS");

  const [columnPrefs, setColumnPrefs] = useState<ColumnPrefsMap>(new Map());

  const getColumnPrefs = useCallback((columnKey: string) => {
    return columnPrefs.get(columnKey);
  }, [columnPrefs]);

  const syncPrefs = () => {
    const stored = getFromStorage(storageKey);

    if (!stored || typeof stored !== "object") return;

    const record = stored as Record<string, ColumnPrefs>;
    const next = new Map(Object.entries(record)) as ColumnPrefsMap;
    setColumnPrefs(next);
  };

  const persistPrefs = (next: ColumnPrefsMap) => {
    const value = Object.fromEntries(next.entries());
    saveToStorage(storageKey, value);
  };

  const updateColumnPref = <K extends keyof ColumnPrefs>(
    columnKey: string,
    prefKey: K,
    value: ColumnPrefs[K],
  ) => {
    setColumnPrefs(prev => {
      const next = new Map(prev);
      const prevPrefs = next.get(columnKey) ?? {};
      const newPrefs = { ...prevPrefs, [prefKey]: value };

      next.set(columnKey, newPrefs);
      persistPrefs(next);
      return next;
    });
  };

  useEffect(() => {
    syncPrefs();
  }, [storageKey]);

  useEventListener("storage", syncPrefs);

  return {
    getColumnPrefs,
    updateColumnPref,
  };
};
