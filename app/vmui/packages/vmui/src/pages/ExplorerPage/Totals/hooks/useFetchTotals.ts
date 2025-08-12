import { explorerTotals } from "../totalsConfig";
import { useAppState } from "../../../../state/common/StateContext";
import { useFetchLogs } from "../../../QueryPage/hooks/useFetchLogs";
import { useEffect } from "preact/compat";
import { useTimeState } from "../../../../state/time/TimeStateContext";
import { useExtraFilters } from "../../FiltersBar/hooks/useExtraFilters";

const statsParts = explorerTotals.map(t => `${t.stats} as ${t.alias}`);
const query = `* | stats ${statsParts.join(", ")}`;

export const useFetchTotals = () => {
  const { serverUrl } = useAppState();
  const { period } = useTimeState();
  const { extraFilterParams } = useExtraFilters();

  console.log(extraFilterParams);

  const { logs, isLoading, error, fetchLogs, abortController } = useFetchLogs(serverUrl, query, 10);

  useEffect(() => {
    if (isLoading || error) return;
    fetchLogs(period);

    return () => {
      abortController.abort();
    };
  }, [period, extraFilterParams]);

  return {
    logs,
    isLoading,
    error,
  };
};
