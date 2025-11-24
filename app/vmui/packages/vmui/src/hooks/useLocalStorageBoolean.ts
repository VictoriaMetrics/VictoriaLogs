import { useMemo, useState, useCallback } from "preact/compat";
import { getFromStorage, saveToStorage, StorageKeys } from "../utils/storage";
import useEventListener from "./useEventListener";

/**
 * A custom hook that synchronizes a boolean state with a value stored in localStorage.
 *
 * @param {StorageKeys} key - The key used to access the corresponding value in localStorage.
 * @param {(key: StorageKeys) => boolean} [customGetter] - Optional custom getter function used to read
 * @returns {[boolean, function]} A tuple containing the current boolean value from localStorage and a setter function to update the value in localStorage.
 *
 * The hook listens to the "storage" event to automatically update the state when the localStorage value changes.
 */
export const useLocalStorageBoolean = (
  key: StorageKeys,
  customGetter?: (key: StorageKeys) => boolean
): [boolean, (value: boolean) => void] => {
  const getter = useCallback((key: StorageKeys) => {
    return customGetter ? customGetter(key) : !!getFromStorage(key);
  }, [key, customGetter]);

  const [value, setValue] = useState(getter(key));

  const handleUpdateStorage = useCallback(() => {
    const newValue = getter(key);
    if (newValue !== value) {
      setValue(newValue);
    }
  }, [key, value, getter]);

  const setNewValue = useCallback((newValue: boolean) => {
    saveToStorage(key, newValue);
  }, [key]);

  useEventListener("storage", handleUpdateStorage);

  return useMemo(() => [value, setNewValue], [value, setNewValue]);
};
