/**
 * Migrates legacy (unprefixed) localStorage keys to the new prefixed format (`${STORAGE_PREFIX}*`).
 * Keeps the prefixed value if it already exists, then removes the legacy key.
 */
import { StorageKeys } from "./types";
import { ALL_STORAGE_KEYS } from "./constants";
import { getFromStorage, removeFromStorage, saveToStorage } from "./storage";

type StorageMigrationResult = {
  migrated: StorageKeys[];
  removed: StorageKeys[];
  skipped: StorageKeys[];
};

export const migrateStorageToPrefixedKeys = (): StorageMigrationResult => {
  const res: StorageMigrationResult = {
    migrated: [],
    removed: [],
    skipped: [],
  };

  for (const key of ALL_STORAGE_KEYS) {
    const legacyKey = key as StorageKeys; // unprefixed
    const legacyValue = getFromStorage(legacyKey, false);
    const prefixedValue = getFromStorage(legacyKey, true);

    if (legacyValue === undefined) {
      res.skipped.push(legacyKey);
      continue;
    }

    // prefixed exists -> keep it, just remove legacy
    if (prefixedValue !== undefined) {
      removeFromStorage([legacyKey], false);
      res.removed.push(legacyKey);
      continue;
    }

    // prefixed missing -> copy legacy -> prefixed, then remove legacy
    saveToStorage(legacyKey, legacyValue, true);
    removeFromStorage([legacyKey], false);
    res.migrated.push(legacyKey);
  }

  return res;
};
