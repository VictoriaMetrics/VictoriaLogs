import { useSearchParams } from "react-router-dom";
import { useCallback, useMemo } from "preact/compat";
import { LOGS_GROUP_BY, LOGS_LIMIT_HITS, LOGS_BAR_COUNT_OPTIONS, LOGS_DEFAULT_BAR_COUNT } from "../../../../constants/logs";

enum  HITS_PARAMS {
  TOP = "top_hits",
  GROUP = "group_hits",
  BARS_COUNT = "bars_count",
}

export const useHitsChartConfig = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const topHits = useMemo(() => {
    const n = Number(searchParams.get(HITS_PARAMS.TOP));
    return Number.isFinite(n) && n > 0 ? n : LOGS_LIMIT_HITS;
  }, [searchParams]);

  const groupFieldHits = searchParams.get(HITS_PARAMS.GROUP) || LOGS_GROUP_BY;

  const barsCount = useMemo(() => {
    const n = Number(searchParams.get(HITS_PARAMS.BARS_COUNT));
    if (!Number.isFinite(n)) return LOGS_DEFAULT_BAR_COUNT;
    return LOGS_BAR_COUNT_OPTIONS.includes(n) ? n : LOGS_DEFAULT_BAR_COUNT;
  }, [searchParams]);

  const setValue = useCallback((param: HITS_PARAMS, newValue?: string) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      const currentValue = prev.get(param);

      if (newValue && newValue !== currentValue) {
        next.set(param, newValue);
      } else {
        next.delete(param);
      }

      return next;
    });
  }, [setSearchParams]);

  const setTopHits = useCallback((newValue?: number) => {
    setValue(HITS_PARAMS.TOP, String(newValue));
  }, [setValue]);

  const setGroupFieldHits = useCallback((newValue?: string) => {
    setValue(HITS_PARAMS.GROUP, newValue);
  }, [setValue]);

  const setBarsCount = useCallback((newValue?: number) => {
    setValue(HITS_PARAMS.BARS_COUNT, newValue ? String(newValue) : undefined);
  }, [setValue]);

  return {
    topHits,
    setTopHits,
    groupFieldHits,
    setGroupFieldHits,
    barsCount,
    setBarsCount
  };
};
