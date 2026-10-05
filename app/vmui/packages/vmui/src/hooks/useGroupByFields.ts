import { useCallback, useEffect, useMemo, useState } from "preact/compat";
import { useTenant } from "./useTenant";
import { useExtraFilters } from "../components/ExtraFilters/hooks/useExtraFilters";
import { useTimePeriod } from "../pages/QueryPage/hooks/useTimePeriod";
import { useHitsChartConfig } from "../pages/QueryPage/HitsPanel/hooks/useHitsChartConfig";
import { getFieldNamesCacheKey, useFetchFieldNames } from "../pages/OverviewPage/hooks/useFetchFieldNames";
import { useOverviewState } from "../state/overview/OverviewStateContext";
import useEventListener from "./useEventListener";
import { addRecentGroupByField, buildGroupByOptions, getRecentGroupByFields } from "../utils/groupByFields";
import { TimeParams } from "../types";

interface Options {
  query?: string;
  // Selectors with the same period share cached field names, while useTimePeriod() resolves a new end time per call.
  period?: TimeParams;
  loadedFieldNames?: string[];
}

const EMPTY_FIELDS: string[] = [];

export const useGroupByFields = ({ query, period, loadedFieldNames = EMPTY_FIELDS }: Options) => {
  const tenant = useTenant();
  const { extraParams } = useExtraFilters();
  const { period: currentPeriod } = useTimePeriod();
  const { start, end } = period ?? currentPeriod;
  const { groupFieldHits: { value: groupBy, set: setGroupBy } } = useHitsChartConfig();
  const { fetchFieldNames, fieldNames, loading, error } = useFetchFieldNames();
  const { fieldNamesCache } = useOverviewState();

  // Read the cache during render, so an already fetched list is shown on the first frame.
  const cachedFieldNames = fieldNamesCache.get(getFieldNamesCacheKey({ period: { start, end }, query, extraParams }, tenant));

  const [recentFields, setRecentFields] = useState<string[]>(() => getRecentGroupByFields(tenant));

  const refreshRecentFields = useCallback(() => {
    setRecentFields(getRecentGroupByFields(tenant));
  }, [tenant]);

  useEffect(refreshRecentFields, [refreshRecentFields]);
  useEventListener("storage", refreshRecentFields);

  const loadFields = useCallback(() => {
    // Updating the latest result re-renders the Overview field names table, which may scroll the page and close the dropdown.
    void fetchFieldNames({ period: { start, end }, extraParams, skipNoiseFields: true, query, updateLatest: false });
  }, [start, end, extraParams.toString(), fetchFieldNames, query]);

  const selectField = useCallback((field: string) => {
    setGroupBy(field);
    setRecentFields(addRecentGroupByField(tenant, field));
  }, [tenant, setGroupBy]);

  const options = useMemo(() => {
    // Until the request succeeds, the fetched fields may belong to the previous params.
    const fetchedRows = cachedFieldNames ?? (loading || error ? [] : fieldNames);
    const fetchedFields = fetchedRows.map(f => f.value);
    return buildGroupByOptions({
      current: groupBy,
      recent: recentFields,
      loaded: loadedFieldNames,
      fetched: fetchedFields,
    });
  }, [groupBy, recentFields, loadedFieldNames, cachedFieldNames, fieldNames, loading, error]);

  return {
    value: groupBy,
    options,
    isLoading: loading,
    error: error ? String(error) : "",
    loadFields,
    selectField,
  };
};
