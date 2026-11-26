import { explorerTotals } from "../totalsConfig";
import { useFetchLogs } from "../../../QueryPage/hooks/useFetchLogs";
import { useEffect, useState } from "preact/compat";
import { useExtraFilters } from "../../../../components/ExtraFilters/hooks/useExtraFilters";
import { getPreviousRange } from "../../../../utils/time";
import { Logs } from "../../../../api/types";
import { TimeParams } from "../../../../types";
import { useTimePeriod } from "../../../QueryPage/hooks/useTimePeriod";
import { useFetchWithTenants } from "../../../../hooks/useFetchWithTenants";

const statsParts = explorerTotals.map(t => t.statsExpr);
const query = `* | ${statsParts.join(", ")}`;

export const useFetchTotals = () => {
  const { period } = useTimePeriod();
  const { extraParams } = useExtraFilters();
  const { tenantRequestKey } = useFetchWithTenants();

  const [totals, setTotals] = useState<Logs>();
  const [totalsPrev, setTotalsPrev] = useState<Logs>();
  const [periods, setPeriods] = useState<{curr: TimeParams, prev: TimeParams}>();

  const { isLoading, error, fetchLogs, abort } = useFetchLogs();

  useEffect(() => {
    let isCurrent = true;
    abort();
    setTotals(undefined);
    setTotalsPrev(undefined);

    async function fetchTotals () {
      try {
        const prevPeriod = getPreviousRange(period);

        const [currRes, prevRes] = await Promise.all([
          fetchLogs({ query, limit: 1, period, extraParams }),
          fetchLogs({ query, limit: 1, period: prevPeriod, extraParams }),
        ]);
        if (!isCurrent) return;

        const [curr] = currRes.data ?? [];
        const [prev] = prevRes.data ?? [];

        setTotals(curr);
        setTotalsPrev(prev);
        setPeriods({ curr: period, prev: prevPeriod });
      } catch (e) {
        console.error(e);
      }
    }

    void fetchTotals();

    return () => {
      isCurrent = false;
      abort();
    };
  }, [period, extraParams.toString(), tenantRequestKey]);

  return {
    totals,
    totalsPrev,
    periods,
    isLoading,
    error,
  };
};
