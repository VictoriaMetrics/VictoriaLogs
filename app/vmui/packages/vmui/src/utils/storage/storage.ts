import { StorageKeys, StorageValue } from "./types";
import { toPrefixedKey } from "./utils";

export const saveToStorage = (key: StorageKeys, value: StorageValue, withPrefix = true): void => {
  try {
    const storageKey = withPrefix ? toPrefixedKey(key) : key;

    if (value) {
      // keeping object in storage so that keeping the string is not different from keeping
      window.localStorage.setItem(storageKey, JSON.stringify({ value }));
    } else {
      window.localStorage.removeItem(storageKey);
    }
    window.dispatchEvent(new StorageEvent("storage", { key: storageKey, newValue: JSON.stringify({ value }) }));
  } catch (e) {
    console.error(e);
  }
};

export const getFromStorage = (key: StorageKeys, withPrefix = true): undefined | StorageValue => {
  const storageKey = withPrefix ? toPrefixedKey(key) : key;
  const valueObj = window.localStorage.getItem(storageKey);

  if (valueObj === null) return undefined;

  try {
    return JSON.parse(valueObj)?.value; // see comment in "saveToStorage"
  } catch (e) {
    return valueObj; // fallback for corrupted json
  }
};

export const removeFromStorage = (keys: StorageKeys[], withPrefix = true): void => {
  const storageKeys = withPrefix ? keys.map(toPrefixedKey) : keys;
  storageKeys.forEach(k => window.localStorage.removeItem(k));
};
