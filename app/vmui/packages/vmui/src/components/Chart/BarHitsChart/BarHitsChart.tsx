import { FC, useCallback, useMemo, useState } from "preact/compat";
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
import SelectLimit from "../../Main/Pagination/SelectLimit/SelectLimit";
import { useHitsChartConfig } from "../../../pages/QueryPage/HitsChart/hooks/useHitsChartConfig";
import { useFetchFieldNames } from "../../../pages/OverviewPage/hooks/useFetchFieldNames";
import { useTimeState } from "../../../state/time/TimeStateContext";
import { useExtraFilters } from "../../../pages/OverviewPage/hooks/useExtraFilters";
import { getDurationFromMilliseconds } from "../../../utils/time";
import useDeviceDetect from "../../../hooks/useDeviceDetect";

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
  const { isMobile } = useDeviceDetect();

  const [graphOptions, setGraphOptions] = useState<GraphOptions>({
    graphStyle: GRAPH_STYLES.BAR,
    queryMode: GRAPH_QUERY_MODE.hits,
    stacked: false,
    fill: false,
    hideChart: false,
  });

  const isHitsMode = graphOptions.queryMode === GRAPH_QUERY_MODE.hits;

  const totalHits = useMemo(() => calculateTotalHits(logHits), [logHits]);

  const { extraParams } = useExtraFilters();
  const { period: { start, end } } = useTimeState();
  const { topHits, setTopHits, groupFieldHits, setGroupFieldHits } = useHitsChartConfig();
  const { fetchFieldNames, fieldNames, loading, error } = useFetchFieldNames();

  const fieldNamesOptions = useMemo(() => {
    return fieldNames.map(v => v.value).sort((a, b) => a.localeCompare(b));
  }, [fieldNames]);

  const handleOpenFields = useCallback(() => {
    fetchFieldNames({ start, end, extraParams, showAllFields: true, query });
  }, [start, end, extraParams.toString(), fetchFieldNames, query]);

  return (
    <div
      className={classNames({
        "vm-bar-hits-chart__wrapper": true,
        "vm-bar-hits-chart__wrapper_hidden": graphOptions.hideChart
      })}
    >
      <div className="vm-bar-hits-chart-header">
        <div
          className={classNames({
          "vm-bar-hits-chart-header-info": true,
          "vm-bar-hits-chart-header-info_mobile": isMobile,
        })}
        >
          <SelectLimit
            label="Top hits"
            options={[5, 10, 25, 50]}
            limit={topHits}
            onChange={setTopHits}
          /> |
          {isHitsMode && (
            <>
              <SelectLimit
                searchable
                label="Group by"
                limit={groupFieldHits}
                options={fieldNamesOptions}
                textNoOptions={"No fields found"}
                isLoading={loading}
                error={error ? String(error) : ""}
                onOpenSelect={handleOpenFields}
                onChange={setGroupFieldHits}
              /> |
            </>
          )}
          <p>Total: <b>{totalHits.toLocaleString("en-US")}</b> hits</p>
          {durationMs !== undefined && (
            <>
              |
              <p>Duration: <b>{getDurationFromMilliseconds(durationMs)}</b></p>
            </>
          )}
        </div>
        <BarHitsOptions
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
