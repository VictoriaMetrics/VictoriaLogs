import { useCallback, useEffect, useMemo, useRef, useState } from "preact/compat";
import { getLogsUrl } from "../../../api/logs";
import { ErrorTypes, TimeParams } from "../../../types";
import { Logs } from "../../../api/types";
import dayjs from "dayjs";
import { useTenant } from "../../../hooks/useTenant";
import { useSearchParams } from "react-router-dom";
import { useAppState } from "../../../state/common/StateContext";

interface FetchLogsParams {
  query?: string;
  period?: TimeParams;
  limit?: number;
  preventAbortPrevious?: boolean;
}

interface FetchLogsOptions extends FetchLogsParams {
  signal: AbortSignal;
}

export const useFetchLogs = (defaultQuery?: string, defaultLimit?: number) => {
  const { serverUrl } = useAppState();
  const tenant = useTenant();
  const [searchParams] = useSearchParams();

  const [logs, setLogs] = useState<Logs[]>([]);
  const [isLoading, setIsLoading] = useState<{ [key: number]: boolean }>({});
  const [error, setError] = useState<ErrorTypes | string>();
  const abortControllerRef = useRef(new AbortController());

  const hideLogs = useMemo(() => searchParams.get("hide_logs"), [searchParams]);

  const url = useMemo(() => getLogsUrl(serverUrl), [serverUrl]);

  const getOptions = ({ query, period, limit, signal }: FetchLogsOptions) => {
    if (!query) {
      throw new Error("query is required to /select/logsql/query.");
    }

    const body = new URLSearchParams({
      query: query.trim(),
    });

    if (limit) {
      body.append("limit", String(limit));
    }

    if (period) {
      body.append("start", dayjs(period.start * 1000).tz().toISOString());
      body.append("end", dayjs(period.end * 1000).tz().toISOString());
    }

    return {
      body,
      signal,
      method: "POST",
      headers: {
        ...tenant,
        Accept: "application/stream+json",
      },
    };
  };

  const fetchLogs = useCallback(async ({
    query = defaultQuery,
    limit = defaultLimit,
    period,
    preventAbortPrevious
  }: FetchLogsParams) => {
    !preventAbortPrevious && abortControllerRef.current.abort();
    abortControllerRef.current = new AbortController();
    const { signal } = abortControllerRef.current;

    const id = Date.now();
    setIsLoading(prev => ({ ...prev, [id]: true }));
    setError(undefined);

    try {
      const options = getOptions({ query, limit, period, signal });
      const response = await fetch(url, options);
      const text = await response.text();

      if (!response.ok || !response.body) {
        setError(text);
        setLogs([]);
        return false;
      }

      const data = text.split("\n", defaultLimit).map(parseLineToJSON).filter(line => line) as Logs[];
      setLogs(data);
      return data;
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
  }, [url, defaultQuery, defaultLimit, tenant]);

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
