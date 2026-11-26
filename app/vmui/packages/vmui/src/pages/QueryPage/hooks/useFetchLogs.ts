import { useCallback, useEffect, useMemo, useRef, useState } from "preact/compat";
import { getLogsUrl } from "../../../api/logs";
import { ErrorTypes, TimeParams } from "../../../types";
import { Logs } from "../../../api/types";
import { useTenants } from "../../../hooks/useTenants";
import { useSearchParams } from "react-router-dom";
import { useAppState } from "../../../state/common/StateContext";
import { mergeSearchParams } from "../../../utils/query-string";
import { TenantType } from "../../../components/Configurators/GlobalSettings/TenantsConfiguration/Tenants";
import { nanosToIsoString, secondsToMilliseconds } from "../../../utils/time";
import { useFetchWithTenants } from "../../../hooks/useFetchWithTenants";
import { DEFAULT_TENANT_ID } from "../../../constants/logs";

export interface FetchLogsParams {
  query: string;
  period?: TimeParams;
  limit?: number;
  extraParams?: URLSearchParams;
  beforeFetch?: BeforeFetch;
  isDownload?: boolean;
  onStart?: () => void;
}

export type BeforeFetchResult =
  | { action: "abort"; }
  | { action: "proceed" }
  | { action: "modify"; body: URLSearchParams }

export type BeforeFetch = (body: Readonly<URLSearchParams>) => Promise<BeforeFetchResult>;

type FetchLogsResultStatus = "success" | "error" | "aborted";

export type FetchLogsResult = {
  response?: Response;
  data?: Logs[];
  limit?: number;
  status: FetchLogsResultStatus;
};

export const useFetchLogs = () => {
  const { serverUrl } = useAppState();
  const { fetchWithTenants } = useFetchWithTenants();
  const [searchParams, setSearchParams] = useSearchParams();
  const setSearchParamsRef = useRef(setSearchParams);
  setSearchParamsRef.current = setSearchParams;

  const { tenants, tenantsKey } = useTenants();
  const tenantsKeyRef = useRef(tenantsKey);
  tenantsKeyRef.current = tenantsKey;

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
      body.set("start", nanosToIsoString(period.start));
      body.set("end", nanosToIsoString(period.end));
    }

    return body;
  };

  const buildOptions = ({ signal }: { signal: AbortSignal }) => {
    return {
      signal,
      method: "POST" as const,
      headers: {
        Accept: "application/stream+json",
      },
    };
  };

  const updateTenant = ({ accountId, projectId }: TenantType) => {
    setSearchParamsRef.current(prev => {
      const next = new URLSearchParams(prev);

      // remove legacy accountID and projectID params, and set the new tenant param
      next.delete("accountID");
      next.delete("projectID");

      next.set("tenant", `${accountId}:${projectId}`);

      return next;
    }, { replace: true });
  };

  const fetchLogs = useCallback(async ({
    query,
    limit,
    period,
    extraParams,
    beforeFetch,
    isDownload = false,
    onStart,
  }: FetchLogsParams): Promise<FetchLogsResult> => {
    const controller = new AbortController();
    abortControllerRef.current = controller;
    const { signal } = controller;

    const id = Date.now();
    const options = buildOptions({ signal });
    let effectiveLimit: number | undefined = undefined;

    try {
      let baseBody = buildBody({ query, limit, period, extraParams });

      const preQueryParams = {
        ...Object.fromEntries(baseBody),
        ...options.headers
      };
      setQueryParams(preQueryParams);

      if (beforeFetch) {
        // new instance to avoid mutation of original body
        const decision = await beforeFetch(new URLSearchParams(baseBody));
        // Return early if instructed to abort
        if (decision.action === "abort" || signal.aborted) return { status: "aborted" };
        // Modify the body if instructed to modify
        if (decision.action === "modify") baseBody = mergeSearchParams(baseBody, decision.body, "overwrite");
      }

      const body = extraParams ? mergeSearchParams(baseBody, extraParams, "append") : baseBody;
      effectiveLimit = body.has("limit") ? Number(body.get("limit")) : undefined;
      const tmpQueryParams = { ...Object.fromEntries(body), ...options.headers };

      setIsLoading(prev => ({ ...prev, [id]: true }));
      setError(undefined);

      onStart?.();
      const response = await fetchWithTenants(url, { body, ...options });

      if (isDownload) {
        return { response, status: response.ok ? "success" : "error", limit: effectiveLimit };
      }

      // Sync the tenant from response headers, keeping the requested ID for any missing component.
      const responseAccountId = response.headers.get("AccountID");
      const responseProjectId = response.headers.get("ProjectID");
      const responseHasTenant = (responseAccountId !== null || responseProjectId !== null);
      if (!signal.aborted && tenantsKeyRef.current === tenantsKey && responseHasTenant) {
        const [accountId, projectId] = (tenants[0] || DEFAULT_TENANT_ID).split(":");

        const hasTenantChanged = (accountId !== responseAccountId || projectId !== responseProjectId);
        if (hasTenantChanged) {
          updateTenant({
            accountId: responseAccountId ?? accountId,
            projectId: responseProjectId ?? projectId,
          });
        }
      }

      const duration = response.headers.get("vl-request-duration-seconds");
      setDurationMs(duration ? secondsToMilliseconds(Number(duration)) : undefined);

      const text = await response.text();
      if (!response.ok || !response.body) {
        setError(text);
        setLogs([]);
        return { status: "error", limit: effectiveLimit };
      }

      const data = text.split("\n").map(parseLineToJSON).filter(line => line) as Logs[];
      setLogs(data);
      setQueryParams(tmpQueryParams);
      return { data, status: "success", limit: effectiveLimit };
    } catch (e) {
      const isError = e instanceof Error;
      const isAbortError = signal.aborted || isError && e.name === "AbortError";
      if (!isAbortError) {
        setError(String(e));
        console.error(e);
        setLogs([]);
      }

      if (isAbortError) {
        return { status: "aborted", limit: effectiveLimit };
      }

      return { status: "error", limit: effectiveLimit };
    } finally {
      setIsLoading(prev => {
        // Remove the `id` key from `isLoading` when its value becomes `false`
        const { [id]: _, ...rest } = prev;
        return rest;
      });

    }
  }, [url, tenantsKey, fetchWithTenants]);

  const resetLogs = () => {
    setLogs([]);
    setError(undefined);
    setDurationMs(undefined);
    setQueryParams({});
  };

  useEffect(() => {
    return () => abortControllerRef.current.abort();
  }, []);

  useEffect(() => {
    if (hideLogs) {
      resetLogs();
    }
  }, [hideLogs]);

  return {
    logs,
    queryParams,
    isLoading: Object.values(isLoading).some(s => s),
    error,
    fetchLogs,
    durationMs,
    abort: useCallback(() => abortControllerRef.current?.abort(), []),
    resetLogs,
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
