import { PrefixedStorageKeys, StorageKeys, TableStorageKey } from "./types";
import { STORAGE_PREFIX, TABLE_KEYS } from "./constants";

export const toPrefixedKey = (key: StorageKeys): PrefixedStorageKeys => {
  return `${STORAGE_PREFIX}${key}`;
};

export const getTableStorageKey = (tableId: string, part: keyof typeof TABLE_KEYS): TableStorageKey => {
  return `${TABLE_KEYS[part]}:${tableId}` as const;
};
