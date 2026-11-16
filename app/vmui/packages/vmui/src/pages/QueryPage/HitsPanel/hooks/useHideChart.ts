import { useCallback, useMemo } from "preact/compat";
import { useSearchParams } from "react-router-dom";
import { useLocalStorageBoolean } from "../../../../hooks/useLocalStorageBoolean";

const HIDE_CHART_PARAM = "hide_chart";
const HIDE_CHART_STORAGE_KEY = "LOGS_HIDE_CHART";

export const useHideChart = (): [boolean, (value: boolean) => void] => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [hideChartStorage, setHideChartStorage] = useLocalStorageBoolean(HIDE_CHART_STORAGE_KEY);
  const hideChartUrl = searchParams.get(HIDE_CHART_PARAM) === "true";
  const hideChart = hideChartStorage || hideChartUrl;

  const setHideChart = useCallback((value: boolean) => {
    const next = new URLSearchParams(searchParams);
    value ? next.set(HIDE_CHART_PARAM, "true") : next.delete(HIDE_CHART_PARAM);

    setHideChartStorage(value);
    setSearchParams(next);
  }, [searchParams, setHideChartStorage, setSearchParams]);

  return useMemo(() => [hideChart, setHideChart], [hideChart, setHideChart]);
};
