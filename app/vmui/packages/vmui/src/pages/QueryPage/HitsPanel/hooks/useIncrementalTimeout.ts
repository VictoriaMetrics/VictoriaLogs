import { useMemo, useState } from "preact/compat";
import { durationToMs } from "../../../../components/Configurators/ExecutionControls/AutoRefreshControl/utils";
import { getFromStorage, saveToStorage } from "../../../../utils/storage";
import useEventListener from "../../../../hooks/useEventListener";
import { toPrefixedKey } from "../../../../utils/storage/utils";

const STORAGE_KEY = "LOGS_INCREMENTAL_TIMEOUT";
const DISABLE_VALUE = "Off";
const DEFAULT_VALUE = "3s";
export const INCREMENTAL_OPTIONS = [DISABLE_VALUE, "1s", "3s", "5s", "10s", "30s"];

export const useIncrementalTimeout = () => {
  const setIncrementalTimeout = (timeout: string) => {
    saveToStorage(STORAGE_KEY, timeout);
  };

  const getIncrementalTimeout = () => {
    const value = getFromStorage(STORAGE_KEY);
    const isValid = typeof value === "string" && INCREMENTAL_OPTIONS.includes(value);
    return isValid ? value : DEFAULT_VALUE;
  };

  const handleUpdateStorage = (event: StorageEvent) => {
    if (event.key !== toPrefixedKey(STORAGE_KEY)) return;
    setIncrementalTimeoutLabel(getIncrementalTimeout());
  };

  const [incrementalTimeoutLabel, setIncrementalTimeoutLabel] = useState<string>(getIncrementalTimeout);
  const incrementalTimeoutMs = useMemo(() => durationToMs(incrementalTimeoutLabel), [incrementalTimeoutLabel]);

  useEventListener("storage", handleUpdateStorage);

  return {
    incrementalTimeoutMs,
    incrementalTimeoutLabel: incrementalTimeoutLabel,
    setIncrementalTimeout,
  };
};
