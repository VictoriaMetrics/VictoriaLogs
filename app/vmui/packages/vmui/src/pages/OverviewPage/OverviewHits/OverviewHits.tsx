import { FC, useEffect, useMemo } from "preact/compat";
import { useFetchHits } from "../../QueryPage/hooks/useFetchHits";
import HitsPanel from "../../QueryPage/HitsPanel/HitsPanel";
import { useSearchParams } from "react-router-dom";
import { useExtraFilters } from "../../../components/ExtraFilters/hooks/useExtraFilters";
import { useHitsChartConfig } from "../../QueryPage/HitsPanel/hooks/useHitsChartConfig";
import { useTimePeriod } from "../../QueryPage/hooks/useTimePeriod";

const OverviewHits: FC = () => {
  const [searchParams] = useSearchParams();
  const { period } = useTimePeriod();
  const query = "*";

  const {
    topHits: { value: topHits },
    groupFieldHits: { value: groupFieldHits },
    step: { value: step },
  } = useHitsChartConfig();

  const { extraParams } = useExtraFilters();
  const { fetchHits, ...dataLogHits } = useFetchHits();

  const hideChart = useMemo(() => {
    return Boolean(searchParams.get("hide_chart"));
  }, [searchParams]);

  useEffect(() => {
    if (hideChart) return;

    void fetchHits({
      period,
      extraParams,
      query,
      step,
      field: groupFieldHits,
      fieldsLimit: topHits,
    });

  }, [hideChart, period, extraParams.toString(), step, topHits, groupFieldHits]);

  return (
    <div>
      <HitsPanel
        isOverview
        {...dataLogHits}
        query={query}
        period={period}
        step={step}
      />
    </div>
  );
};

export default OverviewHits;
