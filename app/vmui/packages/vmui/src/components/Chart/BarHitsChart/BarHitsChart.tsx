import { FC, useMemo, useState } from "preact/compat";
import "./style.scss";
import "uplot/dist/uPlot.min.css";
import { AlignedData } from "uplot";
import { TimeParams } from "../../../types";
import { LogHits } from "../../../api/types";
import { GRAPH_QUERY_MODE, GRAPH_STYLES, GraphOptions } from "./types";
import BarHitsOptions from "./BarHitsOptions/BarHitsOptions";
import BarHitsPlot from "./BarHitsPlot/BarHitsPlot";
import { calculateTotalHits } from "../../../utils/logs";
import BarHitsStats from "./BarHitsStats/BarHitsStats";
import { HitsChartAlert } from "../../../pages/QueryPage/HitsChart/hooks/useHitsChartAlert";
import Alert from "../../Main/Alert/Alert";

interface Props {
  logHits: LogHits[];
  data: AlignedData;
  query?: string;
  period: TimeParams;
  durationMs?: number
  isOverview?: boolean;
  alertData: HitsChartAlert;
  setPeriod: ({ from, to }: { from: Date, to: Date }) => void;
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
          setPeriod={setPeriod}
          graphOptions={graphOptions}
        />
      )}
    </div>
  );
};

export default BarHitsChart;
