import { FC, useEffect, useMemo, useRef, useState } from "preact/compat";
import "./style.scss";
import "uplot/dist/uPlot.min.css";
import { AlignedData } from "uplot";
import { TimeParams, TimePeriod } from "../../../types";
import { LogHits } from "../../../api/types";
import { GRAPH_QUERY_MODE, GRAPH_STYLES, GraphOptions } from "./types";
import BarHitsOptions from "./BarHitsOptions/BarHitsOptions";
import BarHitsPlot from "./BarHitsPlot/BarHitsPlot";
import { calculateTotalHits } from "../../../utils/logs";
import BarHitsStats from "./BarHitsStats/BarHitsStats";
import { HitsChartAlert } from "../../../pages/QueryPage/HitsPanel/hooks/useHitsChartAlert";
import Alert from "../../Main/Alert/Alert";
import { timeParamsToDateRange } from "../../../utils/time";

interface Props {
  logHits: LogHits[];
  data: AlignedData;
  query?: string;
  period: TimeParams;
  durationMs?: number
  isOverview?: boolean;
  alertData: HitsChartAlert;
  setPeriod: (nextPeriod: TimePeriod) => void;
}

const BarHitsChart: FC<Props> = ({
  logHits,
  data: _data,
  query,
  period,
  setPeriod,
  durationMs,
  isOverview,
  alertData,
}) => {
  const [graphOptions, setGraphOptions] = useState<GraphOptions>({
    graphStyle: GRAPH_STYLES.BAR,
    queryMode: GRAPH_QUERY_MODE.hits,
    stacked: false,
    cumulative: false,
    fill: false,
    hideChart: false,
  });

  const isHitsMode = graphOptions.queryMode === GRAPH_QUERY_MODE.hits;
  const totalHits = useMemo(() => calculateTotalHits(logHits), [logHits]);

  const currentPeriodRef = useRef(period);
  const chartPeriodChangeRef = useRef(false);
  const periodChangeTimeoutRef = useRef<ReturnType<typeof setTimeout>>();
  const periodBeforeInteractionRef = useRef<TimeParams>();
  const [prevPeriod, setPrevPeriod] = useState<TimeParams>();

  const handleChangePeriod = (nextPeriod: TimePeriod) => {
    chartPeriodChangeRef.current = true;

    if (periodChangeTimeoutRef.current === undefined) {
      periodBeforeInteractionRef.current = currentPeriodRef.current;
    }

    clearTimeout(periodChangeTimeoutRef.current);

    periodChangeTimeoutRef.current = setTimeout(() => {
      setPrevPeriod(periodBeforeInteractionRef.current);
      periodBeforeInteractionRef.current = undefined;
      periodChangeTimeoutRef.current = undefined;
      chartPeriodChangeRef.current = false;
    }, 500);

    setPeriod(nextPeriod);
  };


  const resetPeriodInteraction = () => {
    clearTimeout(periodChangeTimeoutRef.current);
    periodChangeTimeoutRef.current = undefined;
    periodBeforeInteractionRef.current = undefined;
    setPrevPeriod(undefined);
  };

  const handleRevertPeriod = () => {
    if (!prevPeriod) return;

    setPeriod(timeParamsToDateRange(prevPeriod));
    resetPeriodInteraction();
  };

  useEffect(() => {
    currentPeriodRef.current = period;

    if (chartPeriodChangeRef.current) {
      return;
    }

    resetPeriodInteraction();
  }, [period]);

  useEffect(() => resetPeriodInteraction, []);

  return (
    <div className="vm-bar-hits-chart__wrapper">
      <div className="vm-bar-hits-chart-header">
        {!graphOptions.hideChart && (
          <BarHitsStats
            totalHits={totalHits}
            isHitsMode={isHitsMode}
            durationMs={durationMs}
          />
        )}

        <BarHitsOptions
          query={query}
          isHitsMode={isHitsMode}
          isOverview={isOverview}
          prevPeriod={prevPeriod}
          onRevertPeriod={handleRevertPeriod}
          onChange={setGraphOptions}
        />
      </div>

      {alertData && (
        <div className="vm-query-page-chart__empty">
          <Alert {...alertData}>{alertData.message}</Alert>
        </div>
      )}

      {!graphOptions.hideChart && (
        <BarHitsPlot
          logHits={logHits}
          totalHits={totalHits}
          data={_data}
          period={period}
          setPeriod={handleChangePeriod}
          graphOptions={graphOptions}
        />
      )}
    </div>
  );
};

export default BarHitsChart;
