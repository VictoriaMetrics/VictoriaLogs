import { useCallback, useEffect, useMemo, useRef, useState } from "preact/compat";
import { getLogsUrl } from "../../../api/logs";
import { ErrorTypes, TimeParams } from "../../../types";
import { Logs } from "../../../api/types";
import dayjs from "dayjs";
import { useTenant } from "../../../hooks/useTenant";
import { useSearchParams } from "react-router-dom";

export const useFetchLogs = (server: string, query: string, limit: number) => {
  const tenant = useTenant();
  const [searchParams] = useSearchParams();

  const [logs, setLogs] = useState<Logs[]>([]);
  const [isLoading, setIsLoading] = useState<{ [key: number]: boolean }>({});
  const [error, setError] = useState<ErrorTypes | string>();
  const abortControllerRef = useRef(new AbortController());

  const hideLogs = useMemo(() => searchParams.get("hide_logs"), [searchParams]);

  const url = useMemo(() => getLogsUrl(server), [server]);

  const getOptions = (query: string, period: TimeParams, limit: number, signal: AbortSignal) => ({
    signal,
    method: "POST",
    headers: {
      ...tenant,
      Accept: "application/stream+json",
    },
    body: new URLSearchParams({
      query: query.trim(),
      limit: `${limit}`,
      start: dayjs(period.start * 1000).tz().toISOString(),
      end: dayjs(period.end * 1000).tz().toISOString()
    })
  });

  const fetchLogs = useCallback(async (period: TimeParams) => {
    abortControllerRef.current.abort();
    abortControllerRef.current = new AbortController();
    const { signal } = abortControllerRef.current;

    const id = Date.now();
    setIsLoading(prev => ({ ...prev, [id]: true }));
    setError(undefined);

    try {
      const options = getOptions(query, period, limit, signal);
      const response = await fetch(url, options);
      const text = await response.text();

      if (!response.ok || !response.body) {
        setError(text);
        setLogs([]);
        return false;
      }

      const data = text.split("\n", limit).map(parseLineToJSON).filter(line => line) as Logs[];
      setLogs(data);
      return true;
    } catch (e) {
      if (e instanceof Error && e.name !== "AbortError") {
        setError(String(e));
        console.error(e);
        setLogs([]);
      }
      return false;
    } finally {
      setIsLoading(prev => {
        // Remove the `id` key from `isLoading` when its value becomes `false`
        const { [id]: _, ...rest } = prev;
        return rest;
      });
    }
  }, [url, query, limit, tenant]);

  useEffect(() => {
    return () => {
      abortControllerRef.current.abort();
    };
  }, []);

  useEffect(() => {
    if (hideLogs) {
      setLogs([]);
      setError(undefined);
    }
  }, [hideLogs]);

  return {
    logs,
    isLoading: Object.values(isLoading).some(s => s),
    error,
    fetchLogs,
    abortController: abortControllerRef.current
  };
};

const parseLineToJSON = (line: string): Logs | null => {
  try {
    return line && JSON.parse(line);
  } catch (e) {
    console.error(`Failed to parse "${line}" to JSON\n`, e);
    return null;
  }
};
