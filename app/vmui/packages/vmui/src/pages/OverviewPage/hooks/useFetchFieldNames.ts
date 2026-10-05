import { useState, useCallback, useEffect, useRef } from "preact/hooks";
import { useAppState } from "../../../state/common/StateContext";
import { LogsFieldValues } from "../../../api/types";
import { useOverviewDispatch, useOverviewState } from "../../../state/overview/OverviewStateContext";
import { useTenant } from "../../../hooks/useTenant";
import { TimeParams } from "../../../types";
import { NOISE_FIELDS } from "../../../constants/logs";

interface FetchOptions {
  period: TimeParams;
  query?: string;
  extraParams?: URLSearchParams;
  skipNoiseFields?: boolean;
  skipStreamFields?: boolean;
  // Whether the result is shown by the field names table and the cardinality card.
  updateLatest?: boolean;
}

const STREAM_FIELDS = ["_stream", "_stream_id"];

type FieldNamesParamsOptions = Pick<FetchOptions, "period" | "query" | "extraParams">;

const getFieldNamesParams = ({ period, query, extraParams }: FieldNamesParamsOptions) => {
  const baseParams = new URLSearchParams({
    start: period.start.toString(),
    end: period.end.toString(),
    query: query || "*"
  });
  return new URLSearchParams([...baseParams, ...(extraParams ?? [])]);
};

export const getFieldNamesCacheKey = (options: FieldNamesParamsOptions, tenant: Record<string, string>) => {
  return getFieldNamesParams(options).toString() + JSON.stringify(tenant);
};

export const useFetchFieldNames = () => {
  const { serverUrl } = useAppState();
  const { fieldNamesCache, fieldNamesParamsKey } = useOverviewState();
  const dispatch = useOverviewDispatch();
  const tenant = useTenant();

  const [fieldNames, setFieldNames] = useState<LogsFieldValues[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | string>("");

  const requestIdRef = useRef(0);
  const abortControllerRef = useRef(new AbortController());

  // Read the cache via the ref, so cache updates don't re-create fetchFieldNames and re-run effects depending on it.
  const cacheStateRef = useRef({ fieldNamesCache, fieldNamesParamsKey });
  cacheStateRef.current = { fieldNamesCache, fieldNamesParamsKey };

  const setterFieldNames = (values: LogsFieldValues[], skipNoiseFields = true, skipStreamFields = false) => {
    const noiseFields = skipNoiseFields ? NOISE_FIELDS : [];
    const streamFields = skipStreamFields ? STREAM_FIELDS : [];
    const skipFields = noiseFields.concat(streamFields);
    const filteredFieldNames = !skipFields.length
      ? values
      : values.filter(v => !skipFields.includes(v.value));
    setFieldNames(filteredFieldNames);
  };

  const fetchFieldNames = useCallback(async (options: FetchOptions): Promise<void> => {
    abortControllerRef.current.abort();
    abortControllerRef.current = new AbortController();
    const { signal } = abortControllerRef.current;

    const requestId = ++requestIdRef.current;
    const isLatestRequest = () => requestIdRef.current === requestId;

    setLoading(true);
    setError("");

    try {
      const params = getFieldNamesParams(options);
      const headers = { ...tenant };
      const cacheKey = getFieldNamesCacheKey(options, tenant);
      const updateLatest = options.updateLatest ?? true;
      const { fieldNamesCache, fieldNamesParamsKey } = cacheStateRef.current;

      const cachedFieldNames = fieldNamesCache.get(cacheKey);
      if (cachedFieldNames) {
        if (updateLatest && fieldNamesParamsKey !== cacheKey) {
          dispatch({
            type: "SET_FIELD_NAMES",
            payload: { rows: cachedFieldNames, key: cacheKey }
          });
        }
        setterFieldNames(cachedFieldNames, options.skipNoiseFields, options.skipStreamFields);
        return;
      }

      const url = `${serverUrl}/select/logsql/field_names`;
      const response = await fetch(url, {
        signal,
        method: "POST",
        headers,
        body: params,
      });

      if (!response.ok) {
        const errorResponse = await response.text();
        if (!isLatestRequest()) return;
        const error = `${response.status} ${response.statusText}: ${errorResponse}`;
        console.error(error);
        setError(error);
        return;
      }

      const data: { values: LogsFieldValues[] } = await response.json();
      if (!isLatestRequest()) return;
      setterFieldNames(data.values, options.skipNoiseFields, options.skipStreamFields);
      dispatch({
        type: updateLatest ? "SET_FIELD_NAMES" : "CACHE_FIELD_NAMES",
        payload: { rows: data.values, key: cacheKey }
      });
    } catch (err) {
      if (signal.aborted || !isLatestRequest()) return;
      console.error(err);
      setError(err as Error);
    } finally {
      if (isLatestRequest()) setLoading(false);
    }
  }, [serverUrl, tenant, dispatch]);

  useEffect(() => {
    return () => abortControllerRef.current.abort();
  }, []);

  return {
    fieldNames,
    loading,
    error,
    fetchFieldNames
  };
};
