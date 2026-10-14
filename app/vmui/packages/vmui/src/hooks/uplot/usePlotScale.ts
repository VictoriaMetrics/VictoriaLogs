import { useEffect, useMemo, useState } from "preact/compat";
import { MinMax, TimeParams, TimePeriod } from "../../types";
import {
  nanosecondsToSeconds,
  secondsToNanoseconds,
  timeParamsToDateRange,
} from "../../utils/time";
import { normalizeXRange } from "../../utils/uplot";

const limitsDurations = { min: 0.001, max: 1.578e+8 }; // min: 1 ms, max: 5 years

interface PlotScaleHook {
  setPeriod: (nextPeriod: TimePeriod) => void;
  period: TimeParams;
}

const usePlotScale = ({ period, setPeriod }: PlotScaleHook) => {
  const [xRangeNano, setXRangeNano] = useState({
    min: period.start,
    max: period.end,
  });

  const xRange: MinMax = useMemo(() => {
    return normalizeXRange({
      min: nanosecondsToSeconds(xRangeNano.min),
      max: nanosecondsToSeconds(xRangeNano.max),
    }, limitsDurations.min);
  }, [xRangeNano]);

  const setPlotScale = ({ min, max }: MinMax) => {
    const delta = max - min;

    if (delta < limitsDurations.min || delta > limitsDurations.max) return;

    setPeriod(timeParamsToDateRange({
      start: secondsToNanoseconds(min),
      end: secondsToNanoseconds(max),
    }));
  };

  useEffect(() => {
    setXRangeNano({
      min: period.start,
      max: period.end,
    });
  }, [period]);

  return {
    xRange,
    setPlotScale,
  };
};

export default usePlotScale;
