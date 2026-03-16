import { useCallback, useMemo } from "preact/compat";
import { useSearchParams } from "react-router-dom";
import { ExtraFilter } from "../types";
import { extraFilterOperatorToToken } from "../utils/operatorToken";
import { isStreamFilter } from "../utils/isStreamFilter";
import { EXTRA_FILTERS_KEY, EXTRA_STREAM_FILTERS_KEY } from "../constants";
import { buildExtraFilterParams } from "../utils/buildExtraFilterParams";
import { parseExtraFiltersFromSearchParams } from "../utils/parseExtraFiltersFromSearchParams";

export const useExtraFilters = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const extraFilters: ExtraFilter[] = useMemo(() => {
    const filters = parseExtraFiltersFromSearchParams(searchParams, EXTRA_FILTERS_KEY);
    const streamFilters = parseExtraFiltersFromSearchParams(searchParams, EXTRA_STREAM_FILTERS_KEY);
    return [...filters, ...streamFilters];
  }, [searchParams]);

  const extraParams = useMemo(() => {
    return buildExtraFilterParams(extraFilters);
  }, [extraFilters]);

  const setNewFilters = useCallback((filters: ExtraFilter[]) => {
    const next = new URLSearchParams(searchParams);
    next.delete(EXTRA_FILTERS_KEY);
    next.delete(EXTRA_STREAM_FILTERS_KEY);

    for (const f of filters) {
      next.append(
        isStreamFilter(f) ? EXTRA_STREAM_FILTERS_KEY : EXTRA_FILTERS_KEY,
        JSON.stringify({ f: f.field, o: extraFilterOperatorToToken[f.operator], v: f.value })
      );
    }

    setSearchParams(next, { replace: true });
  }, [searchParams, setSearchParams]);

  const addNewFilter = useCallback((newFilter: ExtraFilter) => {
    const index = extraFilters.findIndex(
      f => f.field === newFilter.field && f.value === newFilter.value
    );

    if (index === -1) {
      setNewFilters([...extraFilters, newFilter]);
      return;
    }

    const next = [...extraFilters];
    next[index] = newFilter;
    setNewFilters(next);
  }, [extraFilters, setNewFilters]);

  const updateFilter = useCallback((filter: ExtraFilter, index: number) => {
    const next = [...extraFilters];
    next[index] = filter;
    setNewFilters(next);
  }, [extraFilters, setNewFilters]);

  const upsertFilters = useCallback((newExtraFilters: ExtraFilter[]) => {
    const byKey = new Map<string, ExtraFilter>();

    for (const f of extraFilters) {
      byKey.set(`${f.field}\u0000${f.value}`, f);
    }

    for (const nf of newExtraFilters) {
      byKey.set(`${nf.field}\u0000${nf.value}`, nf);
    }

    const next = Array.from(byKey.values());
    setNewFilters(next);
  }, [extraFilters, setNewFilters]);

  const removeFilter = useCallback((index: number) => {
    const next = extraFilters.filter((_f, i) => i !== index);
    setNewFilters(next);
  }, [extraFilters, setNewFilters]);

  const removeFilterByValue = useCallback((field: string, value: string) => {
    const next = extraFilters.filter(f => !(f.field === field && f.value === value));
    setNewFilters(next);
  }, [extraFilters, setNewFilters]);

  const removeFilterByField = useCallback((field: string) => {
    const next = extraFilters.filter(f => f.field !== field);
    setNewFilters(next);
  }, [extraFilters, setNewFilters]);

  const clearFilters = useCallback(() => setNewFilters([]), [setNewFilters]);

  return {
    extraFilters,
    extraParams,
    addNewFilter,
    updateFilter,
    upsertFilters,
    removeFilter,
    removeFilterByValue,
    removeFilterByField,
    clearFilters,
  };
};


