import { FC, useMemo } from "preact/compat";
import "./style.scss";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import classNames from "classnames";
import { LogHits } from "../../../api/types";
import dayjs from "dayjs";
import { useTimeDispatch } from "../../../state/time/TimeStateContext";
import { AlignedData } from "uplot";
import BarHitsChart from "../../../components/Chart/BarHitsChart/BarHitsChart";
import Alert from "../../../components/Main/Alert/Alert";
import { TimeParams } from "../../../types";
import LineLoader from "../../../components/Main/LineLoader/LineLoader";
import { useSearchParams } from "react-router-dom";
import { ExtraFilter } from "../../OverviewPage/FiltersBar/types";
import { toEpochSeconds } from "../../../utils/time";

interface Props {
  query: string;
  logHits: LogHits[];
  durationMs?: number;
  period: TimeParams;
  error?: string;
  isLoading: boolean;
  isOverview?: boolean;
  onApplyFilter: (value: ExtraFilter) => void;
}

const HitsChart: FC<Props> = ({ query, logHits, durationMs, period, error, isLoading, isOverview, onApplyFilter }) => {
  const { isMobile } = useDeviceDetect();
  const timeDispatch = useTimeDispatch();
  const [searchParams] = useSearchParams();
  const hideChart = useMemo(() => searchParams.get("hide_chart"), [searchParams]);

  const getYAxes = (logHits: LogHits[], timestamps: number[]) => {
    return logHits.map(hits => {
      const timestampValueMap = new Map();
      hits.timestamps.forEach((ts, idx) => {
        timestampValueMap.set(toEpochSeconds(ts), hits.values[idx] || null);
      });

      return timestamps.map(t => timestampValueMap.get(t) || null);
    });
  };

  const generateTimestamps = (logHits: LogHits[]) => {
    const ts = logHits.map(h => h.timestamps).flat();
    const tsUniq = Array.from(new Set(ts));
    const tsNumber = tsUniq.map(t => toEpochSeconds(dayjs(t)));
    return tsNumber.sort((a, b) => a - b);
  };

  const data = useMemo(() => {
    if (!logHits.length) return [[], []] as AlignedData;
    const xAxis = generateTimestamps(logHits);
    const yAxes = getYAxes(logHits, xAxis);
    return [xAxis, ...yAxes] as AlignedData;
  }, [logHits]);

  const noDataMessage: string = useMemo(() => {
    if (isLoading) return "";

    const noData = data.every(d => d.length === 0);
    const noTimestamps = data[0].length === 0;
    const noValues = data[1].length === 0;
    if (hideChart) {
      return "Chart hidden. Hits updates paused.";
    } else if (noData) {
      return "No logs volume available\nNo volume information available for the current queries and time range.";
    } else if (noTimestamps) {
      return "No timestamp information available for the current queries and time range.";
    } else if (noValues) {
      return "No value information available for the current queries and time range.";
    } return "";
  }, [data, hideChart, isLoading]);

  const setPeriod = ({ from, to }: {from: Date, to: Date}) => {
    timeDispatch({ type: "SET_PERIOD", payload: { from, to } });
  };

  return (
    <section
      className={classNames({
        "vm-query-page-chart": true,
        "vm-block": true,
        "vm-block_mobile": isMobile,
      })}
    >
      {isLoading && <LineLoader/>}
      {!error && noDataMessage && (
        <div className="vm-query-page-chart__empty">
          <Alert variant="info">{noDataMessage}</Alert>
        </div>
      )}

      {error && noDataMessage && (
        <div className="vm-query-page-chart__empty">
          <Alert variant="error"><pre>{error}</pre></Alert>
        </div>
      )}

      {data && (
        <BarHitsChart
          isOverview={isOverview}
          logHits={logHits}
          durationMs={durationMs}
          query={query}
          data={data}
          period={period}
          setPeriod={setPeriod}
          onApplyFilter={onApplyFilter}
        />
      )}
    </section>
  );
};

export default HitsChart;
