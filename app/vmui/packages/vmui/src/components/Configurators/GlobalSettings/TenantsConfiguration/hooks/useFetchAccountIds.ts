import { useAppState } from "../../../../../state/common/StateContext";
import { useEffect, useMemo, useState } from "preact/compat";
import { ErrorTypes } from "../../../../../types";
import { getAccountIds } from "../../../../../api/accountId";

type TenantId = {
  account_id: number;
  project_id: number;
}

export const useFetchAccountIds = () => {
  const { serverUrl } = useAppState();

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<ErrorTypes | string>();
  const [accountIds, setAccountIds] = useState<string[]>([]);

  const fetchUrl = useMemo(() => getAccountIds(serverUrl), [serverUrl]);

  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true);
      try {
        const response = await fetch(fetchUrl);
        const resp = await response.json() as TenantId[];
        const tenants = resp.map(({ account_id, project_id }) => `${account_id}:${project_id}`);
        setAccountIds(tenants.sort((a, b) => a.localeCompare(b)));
      } catch (e) {
        if (e instanceof Error) {
          setError(`${e.name}: ${e.message}`);
        }
      }
      setIsLoading(false);
    };

    fetchData().catch(console.error);
  }, [fetchUrl]);

  return { accountIds, isLoading, error };
};
