import { useAppState } from "../../../../../state/common/StateContext";
import { useMemo, useRef, useState, useCallback } from "preact/compat";
import { ErrorTypes } from "../../../../../types";
import { getTenantsUrl } from "../../../../../api/tenants";
import { useTimePeriod } from "../../../../../pages/QueryPage/hooks/useTimePeriod";

type TenantId = {
  account_id: number;
  project_id: number;
}

export const useFetchTenants = () => {
  const { serverUrl } = useAppState();
  const { period } = useTimePeriod();

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<ErrorTypes | string>();
  const [tenants, setTenants] = useState<string[]>([]);

  const lastRequestKey = useRef<string>();
  const abortControllerRef = useRef<AbortController>();

  const fetchUrl = useMemo(() => getTenantsUrl(serverUrl, period), [serverUrl, period]);

  const fetchTenants = useCallback(async () => {
    setError(undefined);
    abortControllerRef.current?.abort();

    if (lastRequestKey.current === fetchUrl) return;

    setIsLoading(true);

    const currentAbortController = new AbortController();
    abortControllerRef.current = currentAbortController;

    try {
      const response = await fetch(fetchUrl, { signal: currentAbortController.signal });
      if (currentAbortController.signal.aborted) return;

      if (!response.ok) {
        const message = (await response.text()).trim();
        setError(message || `HTTP ${response.status}`);
        return;
      }

      const resp = await response.json() as TenantId[];
      const tenants = resp.map(({ account_id, project_id }) => `${account_id}:${project_id}`);
      setTenants(tenants.sort((a, b) => a.localeCompare(b)));
      lastRequestKey.current = fetchUrl;

    } catch (e) {
      if (!currentAbortController.signal.aborted && e instanceof Error) {
        setError(`${e.name}: ${e.message}`);
      }
    } finally {
      if (abortControllerRef.current === currentAbortController) {
        abortControllerRef.current = undefined;
        setIsLoading(false);
      }
    }
  }, [fetchUrl]);

  const cancelFetchAccountIds = useCallback(() => {
    abortControllerRef.current?.abort();
    abortControllerRef.current = undefined;
    setIsLoading(false);
  }, []);

  return {
    tenants,
    fetchTenants,
    isLoading,
    error,
    cancelFetchAccountIds };
};
