import { useState, useCallback, useEffect, useRef } from "preact/compat";
import { useAppState } from "../../../state/common/StateContext";
import { LogsFieldValues } from "../../../api/types";
import {
  useOverviewDispatch,
  useOverviewState
} from "../../../state/overview/OverviewStateContext";
import { TimeParams } from "../../../types";
import { useFetchWithTenants } from "../../../hooks/useFetchWithTenants";

interface FetchOptions {
  period: TimeParams;
  query?: string;
  extraParams?: URLSearchParams;
  filter?: string;
}

export const useFetchStreamFieldNames = () => {
  const { serverUrl } = useAppState();
  const {
    streamsFieldNames: streamsFieldNamesState,
    streamsFieldNamesParamsKey
  } = useOverviewState();
  const dispatch = useOverviewDispatch();
  const { fetchWithTenants, tenantRequestKey } = useFetchWithTenants();

  const cacheRef = useRef({
    state: streamsFieldNamesState,
    key: streamsFieldNamesParamsKey,
  });

  cacheRef.current = {
    state: streamsFieldNamesState,
    key: streamsFieldNamesParamsKey,
  };

  const abortRef = useRef<AbortController | null>(null);

  const [streamFieldNames, setStreamFieldNames] = useState<LogsFieldValues[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | string>("");

  const fetchStreamFieldNames = useCallback(async (options: FetchOptions): Promise<LogsFieldValues[] | undefined> => {
    abortRef.current?.abort();
    abortRef.current = new AbortController();
    const { signal } = abortRef.current;

    setLoading(true);
    setError("");

    const query = options.query || "*";

    try {
      const params = new URLSearchParams({
        start: options.period.start.toString(),
        end: options.period.end.toString(),
        ignore_pipes: "1",
        query,
      });

      options.extraParams?.forEach((v, k) => params.append(k, v));

      if (options.filter) {
        params.set("filter", options.filter.trim());
      }

      const cacheKey = `${serverUrl}|${params.toString()}|${tenantRequestKey}`;

      if (cacheRef.current.key === cacheKey) {
        setStreamFieldNames(cacheRef.current.state);
        return cacheRef.current.state;
      }

      const url = `${serverUrl}/select/logsql/stream_field_names`;
      const response = await fetchWithTenants(url, {
        method: "POST",
        body: params,
        signal
      });

      if (!response.ok) {
        const errorResponse = await response.text();
        if (signal.aborted) return;
        const e = `${response.status} ${response.statusText}: ${errorResponse}`;
        setStreamFieldNames([]);
        console.error(e);
        setError(e);
        return;
      }

      const data: { values: LogsFieldValues[] } = await response.json();
      if (signal.aborted) return;
      setStreamFieldNames(data.values);
      dispatch({
        type: "SET_STREAM_FIELD_NAMES",
        payload: { rows: data.values, key: cacheKey }
      });
      return data.values;
    } catch (err) {
      if (signal.aborted) return;
      if (err instanceof Error && err.name === "AbortError") return;
      console.error(err);
      setError(err as Error);
      setStreamFieldNames([]);
    } finally {
      if (abortRef.current?.signal === signal) setLoading(false);
    }
  }, [serverUrl, tenantRequestKey, dispatch, fetchWithTenants]);

  useEffect(() => {
    return () => abortRef.current?.abort();
  }, []);

  return {
    streamFieldNames,
    loading,
    error,
    fetchStreamFieldNames,
    abort: useCallback(() => abortRef.current?.abort(), [])
  };
};
