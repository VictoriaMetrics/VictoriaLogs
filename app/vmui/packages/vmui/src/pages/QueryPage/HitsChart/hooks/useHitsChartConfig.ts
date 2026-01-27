import { useSearchParams } from "react-router-dom";
import { useCallback, useMemo } from "preact/compat";
import { LOGS_BAR_COUNT_DEFAULT, LOGS_GROUP_BY, LOGS_LIMIT_HITS } from "../../../../constants/logs";

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

  const barsCount = useMemo(() => {
    const n = Number(searchParams.get(HITS_PARAMS.BARS_COUNT));
    return Number.isFinite(n) && n > 0 ? n : LOGS_BAR_COUNT_DEFAULT;
  }, [searchParams]);

  const groupFieldHits = searchParams.get(HITS_PARAMS.GROUP) || LOGS_GROUP_BY;

  const setValue = useCallback((param: HITS_PARAMS, newValue?: string | number) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      const currentValue = prev.get(param);

      if (newValue && newValue !== currentValue) {
        next.set(param, String(newValue));
      } else {
        next.delete(param);
      }

      return next;
    });
  }, [setSearchParams]);

  const setTopHits = useCallback((value?: number) => {
    setValue(HITS_PARAMS.TOP, value);
  }, [setValue]);

  const setGroupFieldHits = useCallback((value?: string) => {
    setValue(HITS_PARAMS.GROUP, value);
  }, [setValue]);

  const setBarsCount = useCallback((value?: number) => {
    setValue(HITS_PARAMS.BARS_COUNT, value);
  }, [setValue]);

  return {
    topHits: { value: topHits, set: setTopHits },
    groupFieldHits: { value: groupFieldHits, set: setGroupFieldHits },
    barsCount: { value: barsCount, set: setBarsCount },
  };
};
