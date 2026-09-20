import { useEffect, useState, useRef, useCallback, ReactNode } from "preact/compat";
import { ContextData, ContextType } from "../types";
import { SuggestFunctionIcon, SuggestLabelIcon, SuggestMetricIcon, SuggestValueIcon } from "../../../../Main/Icons";
import { AutocompleteOptions } from "../../../../Main/Autocomplete/Autocomplete";
import { useAppState } from "../../../../../state/common/StateContext";
import { useTimePeriod } from "../../../../../pages/QueryPage/hooks/useTimePeriod";
import { AUTOCOMPLETE_LIMITS } from "../../../../../constants/queryAutocomplete";
import { LogsFieldValues } from "../../../../../api/types";
import { useLogsDispatch, useLogsState } from "../../../../../state/logsPanel/LogsStateContext";
import { useTenant } from "../../../../../hooks/useTenant";
import { generateQuery } from "../helpers/utils";
import { DEFAULT_QUERY } from "../../../../../pages/QueryPage/hooks/useQueryController";

type UseLogsQLFetchOptions = {
  contextData?: ContextData;
  extraParams?: URLSearchParams;
}

type FetchDataArgs = {
  urlSuffix: string;
  setter: (value: LogsFieldValues[]) => void;
  params?: URLSearchParams;
}

const icons: Partial<Record<ContextType, ReactNode>> = {
  [ContextType.FilterName]: <SuggestMetricIcon/>,
  [ContextType.FilterUnknown]: <SuggestMetricIcon/>,
  [ContextType.FilterValue]: <SuggestValueIcon/>,
  [ContextType.PipeName]: <SuggestFunctionIcon/>,
  [ContextType.PipeValue]: <SuggestLabelIcon/>,
  [ContextType.Unknown]: <SuggestValueIcon/>,
  [ContextType.FilterOrPipeName]: <SuggestFunctionIcon/>,
};

export const useLogsQLFetchOptions = ({ contextData, extraParams }: UseLogsQLFetchOptions) => {
  const { serverUrl } = useAppState();
  const { period: { start, end } } = useTimePeriod();
  const { autocompleteCache } = useLogsState();
  const dispatch = useLogsDispatch();
  const tenant = useTenant();

  const [loading, setLoading] = useState(false);

  const [fieldNames, setFieldNames] = useState<AutocompleteOptions[]>([]);
  const [fieldValues, setFieldValues] = useState<AutocompleteOptions[]>([]);

  const requestIdRef = useRef(0);

  const abortControllerRef = useRef(new AbortController());

  const getQueryParams = useCallback((params?: Record<string, string>, extra?: URLSearchParams) => {
    const base = new URLSearchParams({
      ...(params || {}),
      limit: `${AUTOCOMPLETE_LIMITS.queryLimit}`,
      start: `${start.toString()}`,
      end: `${end.toString()}`
    });

    return extra?.size ? new URLSearchParams([...base, ...extra]) : base;
  }, [start, end]);

  const processData = (values: LogsFieldValues[], type: ContextType): AutocompleteOptions[] => {
    return values.map(v => ({
      value: v.value,
      type: `${type}`,
      group: "Autocomplete",
      icon: icons[type],
    }));
  };

  const fetchData = async ({ urlSuffix, setter, params }: FetchDataArgs) => {
    abortControllerRef.current.abort();

    abortControllerRef.current = new AbortController();
    const { signal } = abortControllerRef.current;

    const requestId = ++requestIdRef.current;
    const isLatestRequest = () => requestIdRef.current === requestId;

    const tenantString = new URLSearchParams(tenant).toString();
    const key = `${urlSuffix}?${params?.toString()}&${tenantString}`;

    setLoading(true);

    try {
      const cachedData = autocompleteCache.get(key);
      if (cachedData) {
        if (isLatestRequest()) setter(cachedData);
        return;
      }

      const response = await fetch(`${serverUrl}/select/logsql/${urlSuffix}`, {
        signal,
        method: "POST",
        headers: { ...tenant },
        body: params,
      });

      if (response.ok) {
        const data = await response.json();

        if (!isLatestRequest()) return;

        const value = (data?.values || []) as LogsFieldValues[];
        setter(value || []);
        dispatch({ type: "SET_AUTOCOMPLETE_CACHE", payload: { key, value } });
      }
    } catch (e) {
      if (e instanceof Error && e.name !== "AbortError" && isLatestRequest()) {
        dispatch({ type: "SET_AUTOCOMPLETE_CACHE", payload: { key, value: [] } });
        console.error(e);
      }
    } finally {
      if (isLatestRequest()) setLoading(false);
    }
  };

  // fetch field names
  useEffect(() => {
    const validContexts = [ContextType.FilterName, ContextType.FilterUnknown, ContextType.FilterOrPipeName];
    const isInvalidContext = !validContexts.includes(contextData?.contextType || ContextType.Unknown);
    const isDefaultQuery = contextData?.valueContext === DEFAULT_QUERY;

    if (!serverUrl || isInvalidContext || isDefaultQuery) {
      return;
    }

    setFieldNames([]);

    const setter = (filterNames: LogsFieldValues[]) => {
      setFieldNames(processData(filterNames, ContextType.FilterName));
    };

    void fetchData({
      urlSuffix: "field_names",
      setter: setter,
      params: getQueryParams({ query: contextData?.queryBeforeIncompleteFilter || "*" }, extraParams),
    });

    return () => abortControllerRef.current?.abort();
  }, [serverUrl, contextData, getQueryParams, extraParams?.toString()]);

  // fetch field values
  useEffect(() => {
    const isInvalidContext = contextData?.contextType !== ContextType.FilterValue;
    if (!serverUrl || isInvalidContext || !contextData?.filterName) {
      return;
    }

    setFieldValues([]);

    const setter = (filterValues: LogsFieldValues[]) => {
      setFieldValues(processData(filterValues, ContextType.FilterValue));
    };

    void fetchData({
      urlSuffix: "field_values",
      setter: setter,
      params: getQueryParams({ query: generateQuery(contextData), field: contextData.filterName }, extraParams),
    });

    return () => abortControllerRef.current?.abort();
  }, [serverUrl, contextData, getQueryParams, extraParams?.toString()]);

  useEffect(() => {
    return () => {
      requestIdRef.current += 1;
      abortControllerRef.current.abort();
    };
  }, []);

  return {
    fieldNames,
    fieldValues,
    loading,
  };
};
