import { useSearchParams } from "react-router-dom";
import { useCallback, useMemo } from "preact/compat";
import { LOGS_LIMIT_HITS, LOGS_URL_PARAMS } from "../../../../constants/logs";
import { NavigateOptions } from "../../../../types";
import { parseGroupByFields, serializeGroupByFields } from "../../../../utils/logs";

const HITS_PARAMS = {
  TOP: "top_hits",
  GROUP: LOGS_URL_PARAMS.GROUP_BY,
  STEP: "step",
};

export const useHitsChartConfig = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const topHits = useMemo(() => {
    const n = Number(searchParams.get(HITS_PARAMS.TOP));
    return Number.isFinite(n) && n > 0 ? n : LOGS_LIMIT_HITS;
  }, [searchParams]);

  const step = useMemo(() => {
    return searchParams.get(HITS_PARAMS.STEP);
  }, [searchParams]);

  const groupFieldsRaw = searchParams.get(HITS_PARAMS.GROUP);
  const groupFieldHits = useMemo(() => parseGroupByFields(groupFieldsRaw), [groupFieldsRaw]);

  const setValue = useCallback((
    param: string,
    newValue?: string | number,
    navigateOpts?: NavigateOptions
  ) => {
    setSearchParams(prev => {
      const prevValue = prev.get(param);

      const nextValue = newValue ? String(newValue) : null;
      const isEqual = nextValue === prevValue;
      if (isEqual) return prev;

      const next = new URLSearchParams(prev);
      nextValue ? next.set(param, nextValue) : next.delete(param);
      return next;
    }, navigateOpts);
  }, [setSearchParams]);

  const setTopHits = useCallback((value?: number) => {
    setValue(HITS_PARAMS.TOP, value);
  }, [setValue]);

  const setGroupFieldHits = useCallback((fields: string[]) => {
    setValue(HITS_PARAMS.GROUP, fields.length ? serializeGroupByFields(fields) : undefined);
  }, [setValue]);

  const setStep = useCallback((value?: string, navigateOpts?: NavigateOptions) => {
    setValue(HITS_PARAMS.STEP, value, navigateOpts);
  }, [setValue]);

  return {
    topHits: { value: topHits, set: setTopHits },
    groupFieldHits: { value: groupFieldHits, set: setGroupFieldHits },
    step: { value: step, set: setStep },
  };
};
