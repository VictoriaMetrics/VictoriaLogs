import { useState, useCallback, useEffect, useRef } from "preact/hooks";
import { useAppState } from "../../../state/common/StateContext";
import { LogsFieldValues } from "../../../api/types";
import { useTenant } from "../../../hooks/useTenant";
import { TimeParams } from "../../../types";

interface FetchOptions {
  period: TimeParams;
  query?: string;
  field: string;
  extraParams?: URLSearchParams;
  filter?: string;
}

const MAX_CACHE_SIZE = 10;

export const useFetchStreamValues = () => {
  const { serverUrl } = useAppState();
  const tenant = useTenant();

  const cacheRef = useRef<Map<string, LogsFieldValues[]>>(new Map());

  const abortRef = useRef<AbortController | null>(null);

  const [streamValues, setStreamValues] = useState<LogsFieldValues[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | string>("");

  const fetchStreamValues = useCallback(async (options: FetchOptions): Promise<LogsFieldValues[] | undefined> => {
    abortRef.current?.abort();
    abortRef.current = new AbortController();
    const { signal } = abortRef.current;

    const query = options.query || "*";

    const params = new URLSearchParams({
      start: options.period.start.toString(),
      end: options.period.end.toString(),
      ignore_pipes: "1",
      query,
      field: options.field,
    });

    options.extraParams?.forEach((v, k) => params.append(k, v));
    const filter = options.filter?.trim();
    if (filter) params.set("filter", filter);

    const tenantKey = `tenant=${tenant.AccountID}:${tenant.ProjectID}`;
    const cacheKey = `${serverUrl}|${params.toString()}|${tenantKey}`;

    const cacheValue = cacheRef.current.get(cacheKey);
    if (cacheValue !== undefined) {
      cacheRef.current.delete(cacheKey);
      cacheRef.current.set(cacheKey, cacheValue); // LRU cache update
      setStreamValues(cacheValue);
      setError("");
      setLoading(false);
      return cacheValue;
    }

    setLoading(true);
    setError("");

    try {
      const url = `${serverUrl}/select/logsql/stream_field_values`;
      const response = await fetch(url, {
        method: "POST",
        headers: { ...tenant },
        body: params,
        signal
      });

      if (!response.ok) {
        const errorResponse = await response.text();
        const error = `${response.status} ${response.statusText}: ${errorResponse}`;
        console.error(error);
        setError(error);
        return;
      }

      const data: { values: LogsFieldValues[] } = await response.json();
      if (signal.aborted) return;
      setStreamValues(data.values);

      cacheRef.current.set(cacheKey, data.values);
      // Ensure cache does not exceed max size
      while (cacheRef.current.size > MAX_CACHE_SIZE) {
        const firstKey = cacheRef.current.keys().next().value;
        if (firstKey !== undefined) cacheRef.current.delete(firstKey);
      }
      return data.values;
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") return;
      setError(err instanceof Error ? err.message : String(err));
      console.error(err);
    } finally {
      if (abortRef.current?.signal === signal) setLoading(false);
    }
  }, [serverUrl, tenant]);

  useEffect(() => {
    return () => abortRef.current?.abort();
  }, []);

  return {
    abort: useCallback(() => abortRef.current?.abort(), []),
    streamValues,
    loading,
    error,
    fetchStreamValues,
  };
};
