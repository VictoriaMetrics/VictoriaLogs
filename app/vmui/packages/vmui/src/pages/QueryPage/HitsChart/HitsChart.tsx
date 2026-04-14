import { FC, useMemo } from "preact/compat";
import "./style.scss";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import classNames from "classnames";
import { LogHits } from "../../../api/types";
import { useTimeDispatch } from "../../../state/time/TimeStateContext";
import { AlignedData } from "uplot";
import BarHitsChart from "../../../components/Chart/BarHitsChart/BarHitsChart";
import { TimeParams } from "../../../types";
import LineLoader from "../../../components/Main/LineLoader/LineLoader";
import { useSearchParams } from "react-router-dom";
import { getSecondsFromDuration, toEpochSeconds } from "../../../utils/time";
import { useHitsChartAlert } from "./hooks/useHitsChartAlert";

interface Props {
  query: string;
  logHits: LogHits[];
  durationMs?: number;
  period: TimeParams;
  step: string | null;
  error?: string;
  isLoading: boolean;
  isOverview?: boolean;
}

const HitsChart: FC<Props> = ({ query, logHits, durationMs, period, step, error, isLoading, isOverview }) => {
  const { isMobile } = useDeviceDetect();
  const timeDispatch = useTimeDispatch();
  const [searchParams] = useSearchParams();
  const hideChart = useMemo(() => searchParams.get("hide_chart") === "true", [searchParams]);

  const getYAxes = (logHits: LogHits[], timestamps: number[]) => {
    return logHits.map(hits => {
      const timestampValueMap = new Map();
      hits.timestamps.forEach((ts, idx) => {
        timestampValueMap.set(toEpochSeconds(ts), hits.values[idx] || null);
      });

      return timestamps.map(t => timestampValueMap.get(t) || null);
    });
  };

  const fillTimestamps = (timestamps: number[], period: TimeParams) => {
    const { step, start, end } = period;
    if (!step || !timestamps.length) return timestamps;

    const stepSec = getSecondsFromDuration(step);
    const minTime = start;
    const maxTime = end;
    const anchorUnix = timestamps[0];

    const result: number[] = [anchorUnix];

    for (let unix = anchorUnix - stepSec; unix >= minTime; unix -= stepSec) {
      result.unshift(unix);
    }

    for (let unix = anchorUnix + stepSec; unix <= maxTime; unix += stepSec) {
      result.push(unix);
    }

    return result;
  };

  const generateTimestamps = (logHits: LogHits[]) => {
    const ts = logHits.map(h => h.timestamps).flat();
    const tsUniq = Array.from(new Set(ts));
    const tsUnix = tsUniq.map(t => toEpochSeconds(t));
    const tsSorted = tsUnix.sort((a, b) => a - b);
    return fillTimestamps(tsSorted, { ...period, step: step! });
  };

  // Intentionally recompute xAxis only when data changes.
  // Period may change multiple times before fresh data arrives.
  const data = useMemo(() => {
    if (!logHits.length) return [[], []] as AlignedData;
    const xAxis = generateTimestamps(logHits);
    const yAxes = getYAxes(logHits, xAxis);
    return [xAxis, ...yAxes] as AlignedData;
  }, [logHits]);

  const alertData = useHitsChartAlert({ data, error, isLoading, hideChart });

  const setPeriod = ({ from, to }: { from: Date, to: Date }) => {
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

      {data && (
        <BarHitsChart
          isOverview={isOverview}
          logHits={logHits}
          durationMs={durationMs}
          query={query}
          data={data}
          period={period}
          setPeriod={setPeriod}
          alertData={alertData}
        />
      )}
    </section>
  );
};

export default HitsChart;
