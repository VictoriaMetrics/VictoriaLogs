import { FC, useMemo, useState } from "preact/compat";
import "./style.scss";
import "uplot/dist/uPlot.min.css";
import { AlignedData } from "uplot";
import { TimeParams } from "../../../types";
import classNames from "classnames";
import { LogHits } from "../../../api/types";
import { GRAPH_QUERY_MODE, GRAPH_STYLES, GraphOptions } from "./types";
import BarHitsOptions from "./BarHitsOptions/BarHitsOptions";
import BarHitsPlot from "./BarHitsPlot/BarHitsPlot";
import { calculateTotalHits } from "../../../utils/logs";
import { ExtraFilter } from "../../../pages/OverviewPage/FiltersBar/types";
import BarHitsStats from "./BarHitsStats/BarHitsStats";

interface Props {
  logHits: LogHits[];
  data: AlignedData;
  query?: string;
  period: TimeParams;
  durationMs?: number
  isOverview?: boolean;
  setPeriod: ({ from, to }: { from: Date, to: Date }) => void;
  onApplyFilter: (value: ExtraFilter) => void;
}

const BarHitsChart: FC<Props> = ({ logHits, data: _data, query, period, setPeriod, onApplyFilter, durationMs, isOverview }) => {
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
    <div
      className={classNames({
        "vm-bar-hits-chart__wrapper": true,
        "vm-bar-hits-chart__wrapper_hidden": graphOptions.hideChart
      })}
    >
      <div className="vm-bar-hits-chart-header">
        <BarHitsStats
          totalHits={totalHits}
          isHitsMode={isHitsMode}
          durationMs={durationMs}
        />
        <BarHitsOptions
          query={query}
          isHitsMode={isHitsMode}
          isOverview={isOverview}
          onChange={setGraphOptions}
        />
      </div>
      {!graphOptions.hideChart && (
        <BarHitsPlot
          logHits={logHits}
          totalHits={totalHits}
          data={_data}
          period={period}
          setPeriod={setPeriod}
          onApplyFilter={onApplyFilter}
          graphOptions={graphOptions}
        />
      )}
    </div>
  );
};

export default BarHitsChart;
