import { StorageKeys, StorageOptions, StorageValue } from "./types";
import { toPrefixedKey } from "./utils";

const resolveStorageOptions = (options?: StorageOptions) => {
  return {
    withPrefix: options?.withPrefix ?? true,
    storage: options?.storage === "session" ? window.sessionStorage : window.localStorage,
  };
};

export const saveToStorage = (key: StorageKeys, value: StorageValue, options?: StorageOptions): void => {
  try {
    const { withPrefix, storage } = resolveStorageOptions(options);
    const storageKey = withPrefix ? toPrefixedKey(key) : key;

    if (value) {
      // keeping object in storage so that keeping the string is not different from keeping
      storage.setItem(storageKey, JSON.stringify({ value }));
    } else {
      storage.removeItem(storageKey);
    }
    window.dispatchEvent(
      new StorageEvent("storage", {
        key: storageKey,
        newValue: JSON.stringify({ value }),
        storageArea: storage,
      })
    );
  } catch (e) {
    console.error(e);
  }
};

export const getFromStorage = (key: StorageKeys, options?: StorageOptions): undefined | StorageValue => {
  const { withPrefix, storage } = resolveStorageOptions(options);
  const storageKey = withPrefix ? toPrefixedKey(key) : key;
  const valueObj = storage.getItem(storageKey);

  if (valueObj === null) return undefined;

  try {
    return JSON.parse(valueObj)?.value; // see comment in "saveToStorage"
  } catch (e) {
    return valueObj; // fallback for corrupted json
  }
};

export const removeFromStorage = (keys: StorageKeys[], options?: StorageOptions): void => {
  const { withPrefix, storage } = resolveStorageOptions(options);
  const storageKeys = withPrefix ? keys.map(toPrefixedKey) : keys;

  storageKeys.forEach(k => {
    storage.removeItem(k);
    window.dispatchEvent(
      new StorageEvent("storage", {
        key: k,
        storageArea: storage,
      })
    );
  });
};
