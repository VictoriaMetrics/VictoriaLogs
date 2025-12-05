import { useCallback, useEffect, useMemo, useRef, useState } from "preact/compat";
import { getLogsUrl } from "../../../api/logs";
import { ErrorTypes, TimeParams } from "../../../types";
import { Logs } from "../../../api/types";
import dayjs from "dayjs";
import { useTenant } from "../../../hooks/useTenant";
import { useSearchParams } from "react-router-dom";
import { useAppState } from "../../../state/common/StateContext";
import { mergeSearchParams } from "../../../utils/query-string";
import { TenantType } from "../../../components/Configurators/GlobalSettings/TenantsConfiguration/Tenants";

interface FetchLogsParams {
  query?: string;
  period?: TimeParams;
  limit?: number;
  extraParams?: URLSearchParams;
  beforeFetch?: BeforeFetch;
  isDownload?: boolean;
}

export type BeforeFetchResult =
  | { action: "abort"; }
  | { action: "proceed" }
  | { action: "modify"; body: URLSearchParams }

export type BeforeFetch = (body: Readonly<URLSearchParams>) => Promise<BeforeFetchResult>;

export const useFetchLogs = (defaultQuery?: string, defaultLimit?: number) => {
  const { serverUrl } = useAppState();
  const tenant = useTenant();
  const [searchParams, setSearchParams] = useSearchParams();

  const [logs, setLogs] = useState<Logs[]>([]);
  const [queryParams, setQueryParams] = useState<Record<string, string>>({});
  const [durationMs, setDurationMs] = useState<number | undefined>();
  const [isLoading, setIsLoading] = useState<{ [key: number]: boolean }>({});
  const [error, setError] = useState<ErrorTypes | string>();
  const abortControllerRef = useRef(new AbortController());

  const hideLogs = useMemo(() => searchParams.get("hide_logs"), [searchParams]);

  const url = useMemo(() => getLogsUrl(serverUrl), [serverUrl]);

  const buildBody = ({ period, query, limit }: FetchLogsParams) => {
    if (!query) {
      throw new Error("query is required to /select/logsql/query.");
    }

    const body = new URLSearchParams({
      query: query.trim(),
    });

    if (limit) {
      body.set("limit", String(limit));
    }

    if (period) {
      body.set("start", dayjs(period.start * 1000).tz().toISOString());
      body.set("end", dayjs(period.end * 1000).tz().toISOString());
    }

    return body;
  };

  const buildOptions = ({ signal }: { signal: AbortSignal }): RequestInit => {
    return {
      signal,
      method: "POST",
      headers: {
        ...tenant,
        Accept: "application/stream+json",
      },
    };
  };

  const updateTenant = ({ accountId, projectId }: Partial<TenantType>) => {
    if (accountId) searchParams.set("accountID", accountId);
    if (projectId) searchParams.set("projectID", projectId);
    setSearchParams(searchParams);
  };

  const fetchLogs = useCallback(async ({
    query = defaultQuery,
    limit = defaultLimit,
    period,
    extraParams,
    beforeFetch,
    isDownload = false,
  }: FetchLogsParams) => {
    abortControllerRef.current = new AbortController();
    const { signal } = abortControllerRef.current;
    const options = buildOptions({ signal });

    let baseBody = buildBody({ query, limit, period, extraParams });

    const preQueryParams = { ...Object.fromEntries(baseBody), ...options.headers };
    setQueryParams(preQueryParams);

    if (beforeFetch) {
      // new instance to avoid mutation of original body
      const decision = await beforeFetch(new URLSearchParams(baseBody));
      // Return early if instructed to abort
      if (decision.action === "abort") return false;
      // Modify the body if instructed to modify
      if (decision.action === "modify") baseBody = mergeSearchParams(baseBody, decision.body, "overwrite");
    }

    const body = extraParams ? mergeSearchParams(baseBody, extraParams, "append") : baseBody;
    const tmpQueryParams = { ...Object.fromEntries(body), ...options.headers };

    const id = Date.now();
    setIsLoading(prev => ({ ...prev, [id]: true }));
    setError(undefined);

    try {
      const response = await fetch(url,  { body, ...options });

      if (isDownload) {
        return response;
      }

      const uiAccountId = (options.headers as Record<string, string>)?.AccountID;
      const vlAccountId = response.headers.get("AccountID");
      const changedAccountId = vlAccountId && vlAccountId !== uiAccountId;
      if (changedAccountId) updateTenant({ accountId: vlAccountId });

      const uiProjectId = (options.headers as Record<string, string>)?.ProjectID;
      const vlProjectId = response.headers.get("ProjectID");
      const changedProjectId = vlProjectId && vlProjectId !== uiProjectId;
      if (changedProjectId) updateTenant({ projectId: vlProjectId });

      const duration = response.headers.get("vl-request-duration-seconds");
      setDurationMs(duration ? Number(duration) * 1000 : undefined);

      const text = await response.text();
      if (!response.ok || !response.body) {
        setError(text);
        setLogs([]);
        return false;
      }

      const data = text.split("\n").map(parseLineToJSON).filter(line => line) as Logs[];
      setLogs(data);
      setQueryParams(tmpQueryParams);
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
    return () => abortControllerRef.current.abort();
  }, []);

  useEffect(() => {
    if (hideLogs) {
      setLogs([]);
      setError(undefined);
    }
  }, [hideLogs]);

  return {
    logs,
    queryParams,
    isLoading: Object.values(isLoading).some(s => s),
    error,
    fetchLogs,
    durationMs,
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
