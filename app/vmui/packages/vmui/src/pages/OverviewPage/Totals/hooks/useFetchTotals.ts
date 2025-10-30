import { explorerTotals } from "../totalsConfig";
import { useFetchLogs } from "../../../QueryPage/hooks/useFetchLogs";
import { useEffect, useState } from "preact/compat";
import { useTimeState } from "../../../../state/time/TimeStateContext";
import { useExtraFilters } from "../../hooks/useExtraFilters";
import { getPreviousRange } from "../../../../utils/time";
import { Logs } from "../../../../api/types";
import { TimeParams } from "../../../../types";

const statsParts = explorerTotals.map(t => t.statsExpr);
const query = `* | ${statsParts.join(", ")}`;

export const useFetchTotals = () => {
  const { period } = useTimeState();
  const { extraParams } = useExtraFilters();

  const [totals, setTotals] = useState<Logs>();
  const [totalsPrev, setTotalsPrev] = useState<Logs>();
  const [periods, setPeriods] = useState<{curr: TimeParams, prev: TimeParams}>();

  const { isLoading, error, fetchLogs, abortController } = useFetchLogs(query, 1);

  useEffect(() => {
    abortController.abort();
    setTotals(undefined);
    setTotalsPrev(undefined);

    async function fetchTotals () {
      try {
        const prevPeriod = getPreviousRange(period);

        const [currRes, prevRes] = await Promise.all([
          fetchLogs({ period, extraParams }),
          fetchLogs({ period: prevPeriod, extraParams }),
        ]);

        const [curr] = (currRes || []) as Logs[];
        const [prev] = (prevRes || []) as Logs[];

        setTotals(curr);
        setTotalsPrev(prev);
        setPeriods({ curr: period, prev: prevPeriod });
      } catch (e) {
        console.error(e);
      }
    }

    fetchTotals();

    return () => abortController.abort();
  }, [period, extraParams.toString()]);

  return {
    totals,
    totalsPrev,
    periods,
    isLoading,
    error,
  };
};
