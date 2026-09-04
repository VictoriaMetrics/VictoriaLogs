import { FC, useEffect } from "preact/compat";
import { useFetchHits } from "../../QueryPage/hooks/useFetchHits";
import HitsPanel from "../../QueryPage/HitsPanel/HitsPanel";
import { useExtraFilters } from "../../../components/ExtraFilters/hooks/useExtraFilters";
import { useHitsChartConfig } from "../../QueryPage/HitsPanel/hooks/useHitsChartConfig";
import { useTimePeriod } from "../../QueryPage/hooks/useTimePeriod";
import { useHideChart } from "../../QueryPage/HitsPanel/hooks/useHideChart";

const OverviewHits: FC = () => {
  const [hideChart] = useHideChart();
  const { period } = useTimePeriod();
  const query = "*";

  const {
    topHits: { value: topHits },
    groupFieldHits: { value: groupFieldHits },
    step: { value: step },
  } = useHitsChartConfig();

  const { extraParams } = useExtraFilters();
  const { fetchHits, ...dataLogHits } = useFetchHits();

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
