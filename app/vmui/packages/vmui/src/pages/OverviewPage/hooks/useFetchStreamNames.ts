import { useState, useCallback, useEffect, useRef } from "preact/compat";
import { useAppState } from "../../../state/common/StateContext";
import { LogsFieldValues } from "../../../api/types";
import {
  useOverviewDispatch,
  useOverviewState
} from "../../../state/overview/OverviewStateContext";
import { useTenant } from "../../../hooks/useTenant";
import { stripPipes } from "../../../components/Configurators/QueryEditor/LogsQL/parser";

interface FetchOptions {
  start: number;
  end: number;
  query?: string;
  extraParams?: URLSearchParams;
}

export const useFetchStreamFieldNames = () => {
  const { serverUrl } = useAppState();
  const {
    streamsFieldNames: streamsFieldNamesState,
    streamsFieldNamesParamsKey
  } = useOverviewState();
  const dispatch = useOverviewDispatch();
  const tenant = useTenant();

  const abortRef = useRef<AbortController | null>(null);

  const [streamFieldNames, setStreamFieldNames] = useState<LogsFieldValues[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | string>("");

  const fetchStreamFieldNames = useCallback(async (options: FetchOptions): Promise<void> => {
    abortRef.current?.abort();
    abortRef.current = new AbortController();
    const { signal } = abortRef.current;

    setLoading(true);
    setError("");

    // TODO: remove when backend supports `ignore_pipes`.
    // Temporarily drop everything after the first `|`.
    // See: https://github.com/VictoriaMetrics/VictoriaLogs/issues/1156
    // See also: useFetchStreamValues for the same logic.
    const query = stripPipes(options.query || "*");

    try {
      const params = new URLSearchParams({
        start: options.start.toString(),
        end: options.end.toString(),
        query,
      });
      options.extraParams?.forEach((v, k) => params.append(k, v));

      const tenantKey = `tenant=${tenant.AccountID}:${tenant.ProjectID}`;
      const cacheKey = `${serverUrl}|${params.toString()}|${tenantKey}`;

      if (streamsFieldNamesParamsKey === cacheKey) {
        setStreamFieldNames(streamsFieldNamesState);
        return;
      }

      const url = `${serverUrl}/select/logsql/stream_field_names`;
      const response = await fetch(url, {
        method: "POST",
        headers: { ...tenant },
        body: params,
        signal
      });

      if (!response.ok) {
        const errorResponse = await response.text();
        const e = `${response.status} ${response.statusText}: ${errorResponse}`;
        setStreamFieldNames([]);
        console.error(e);
        setError(e);
        return;
      }

      const data: { values: LogsFieldValues[] } = await response.json();
      setStreamFieldNames(data.values);
      dispatch({
        type: "SET_STREAM_FIELD_NAMES",
        payload: { rows: data.values, key: cacheKey }
      });
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") return;
      console.error(err);
      setError(err as Error);
      setStreamFieldNames([]);
    } finally {
      setLoading(false);
    }
  }, [serverUrl, tenant, streamsFieldNamesParamsKey, streamsFieldNamesState, dispatch]);

  useEffect(() => {
    return () => abortRef.current?.abort();
  }, []);

  return {
    streamFieldNames,
    loading,
    error,
    fetchStreamFieldNames
  };
};
