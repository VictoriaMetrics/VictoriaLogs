import { getFromStorage, saveToStorage } from "../../../../utils/storage";
import { useSearchParams } from "react-router-dom";
import { LOGS_DEFAULT_LIMIT, LOGS_URL_PARAMS } from "../../../../constants/logs";
import { useCallback } from "react";
import { useMemo } from "preact/compat";

export const useLimitController = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const rawLimitFromParams = searchParams.get(LOGS_URL_PARAMS.LIMIT);

  const limit = useMemo(() => {
    // 1. Try URL param
    const paramsLimit = Number(rawLimitFromParams);
    if (paramsLimit && !isNaN(paramsLimit)) return paramsLimit;

    // 2. Try session storage
    const storageLimit = Number(getFromStorage("LOGS_LIMIT"));
    if (storageLimit && !isNaN(storageLimit)) return storageLimit;

    // 3. Fallback
    return LOGS_DEFAULT_LIMIT;
  }, [rawLimitFromParams]);

  const setLimit = useCallback((nextLimit: number) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      next.set(LOGS_URL_PARAMS.LIMIT, `${nextLimit}`);
      return next;
    });

    saveToStorage("LOGS_LIMIT", `${nextLimit}`);
  }, [setSearchParams]);

  return {
    limit,
    setLimit,
  };
};
