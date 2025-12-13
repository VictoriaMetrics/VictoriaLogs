import { FC, useEffect, useMemo } from "preact/compat";
import { useFetchLogHits } from "../../QueryPage/hooks/useFetchLogHits";
import HitsChart from "../../QueryPage/HitsChart/HitsChart";
import { useTimeState } from "../../../state/time/TimeStateContext";
import { useSearchParams } from "react-router-dom";
import { useExtraFilters } from "../hooks/useExtraFilters";
import { useHitsChartConfig } from "../../QueryPage/HitsChart/hooks/useHitsChartConfig";

const OverviewHits: FC = () => {
  const [searchParams] = useSearchParams();
  const { period } = useTimeState();
  const query = "*";

  const { topHits, groupFieldHits } = useHitsChartConfig();

  const { extraParams, addNewFilter } = useExtraFilters();
  const { fetchLogHits, ...dataLogHits } = useFetchLogHits();

  const hideChart = useMemo(() => {
    return Boolean(searchParams.get("hide_chart"));
  }, [searchParams]);

  useEffect(() => {
    if (hideChart) return;

    fetchLogHits({
      period,
      extraParams,
      query,
      field: groupFieldHits,
      fieldsLimit: topHits,
    });

  }, [hideChart, period, extraParams.toString(), topHits, groupFieldHits]);

  return (
    <div>
      <HitsChart
        isOverview
        {...dataLogHits}
        query={query}
        period={period}
        onApplyFilter={addNewFilter}
      />
    </div>
  );
};

export default OverviewHits;
