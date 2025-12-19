import { useMemo, useState } from "preact/compat";
import { getAxes, getMinMaxBuffer, handleDestroy, setSelect } from "../../../../utils/uplot";
import uPlot, { AlignedData, Band, Options, Series } from "uplot";
import { getCssVariable } from "../../../../utils/theme";
import { useAppState } from "../../../../state/common/StateContext";
import { MinMax, SetMinMax } from "../../../../types";
import { LogHits } from "../../../../api/types";
import getSeriesPaths from "../../../../utils/uplot/paths";
import { GraphOptions, GRAPH_STYLES } from "../types";
import { getColorFromString } from "../../../../utils/color";

const seriesColors = [
  "color-log-hits-bar-1",
  "color-log-hits-bar-2",
  "color-log-hits-bar-3",
  "color-log-hits-bar-4",
  "color-log-hits-bar-5",
];

const strokeWidth = {
  [GRAPH_STYLES.BAR]: 1,
  [GRAPH_STYLES.LINE_STEPPED]: 2,
  [GRAPH_STYLES.LINE]: 1.2,
  [GRAPH_STYLES.POINTS]: 0,
};

interface UseGetBarHitsOptionsArgs {
  data: AlignedData;
  logHits: LogHits[];
  xRange: MinMax;
  bands?: Band[];
  containerSize: { width: number, height: number };
  setPlotScale: SetMinMax;
  onReadyChart: (u: uPlot) => void;
  graphOptions: GraphOptions;
  timezone: string;
}

export const OTHER_HITS_LABEL = "other fields";

export const getLabelFromLogHit = (logHit: LogHits) => {
  if (logHit?._isOther) return OTHER_HITS_LABEL;
  const fields = Object.values(logHit?.fields || {});
  return fields.map((value) => value || "\"\"").join(", ");
};

const getYRange = (u: uPlot, initMin = 0, initMax = 1) => {
  const ySeries = u.series.filter(({ scale }) => scale === "y");

  let min = Infinity;
  let max = -Infinity;

  for (const s of ySeries) {
    const sMin = Number.isFinite(s.min) ? (s.min as number) : initMin;
    const sMax = Number.isFinite(s.max) ? (s.max as number) : initMax;

    if (sMin < min) min = sMin;
    if (sMax > max) max = sMax;
  }

  let lo = Number.isFinite(min) ? min : initMin;
  let hi = Number.isFinite(max) ? max : initMax;

  // If the whole dataset is non-negative, anchor the lower bound at 0
  if (lo >= 0) lo = 0;

  // If the whole dataset is non-positive, anchor the upper bound at 0
  if (hi <= 0) hi = 0;

  return getMinMaxBuffer(lo, hi);
};

const useBarHitsOptions = ({
  data,
  logHits,
  xRange,
  bands,
  containerSize,
  onReadyChart,
  setPlotScale,
  graphOptions,
  timezone,
}: UseGetBarHitsOptionsArgs) => {
  const { isDarkTheme } = useAppState();

  const [focusDataIdx, setFocusDataIdx] = useState(-1);

  const setCursor = (u: uPlot) => {
    const dataIdx = u.cursor.idx ?? -1;
    setFocusDataIdx(dataIdx);
  };

  const series: Series[] = useMemo(() => {
    let visibleColorIndex = 0;

    return data.map((_d, i) => {
      if (i === 0) return {}; // x-axis

      const logHit = logHits?.[i - 1];
      const label = getLabelFromLogHit(logHit);
      const isOther = logHit?._isOther;
      const colorVar = isOther
        ? "color-log-hits-bar-0"
        : seriesColors[visibleColorIndex];

      const color = visibleColorIndex >= 5 ? getColorFromString(label) : getCssVariable(colorVar);

      if (!isOther) visibleColorIndex += 1;

      return {
        label,
        width: strokeWidth[graphOptions.graphStyle],
        spanGaps: true,
        show: true,
        stroke: color,
        fill: graphOptions.fill && !isOther ? `${color}80` : graphOptions.fill ? color : "",
        paths: getSeriesPaths(graphOptions.graphStyle),
      };
    });
  }, [isDarkTheme, data, graphOptions]);

  const options: Options = {
    series,
    bands,
    width: containerSize.width || (window.innerWidth / 2),
    height: containerSize.height || 200,
    cursor: {
      points: {
        width: (u, seriesIdx, size) => size / 4,
        size: (u, seriesIdx) => (u.series?.[seriesIdx]?.points?.size || 1) * 1.5,
        stroke: (u, seriesIdx) => `${series?.[seriesIdx]?.stroke || "#ffffff"}`,
        fill: () => "#ffffff",
      },
    },
    scales: {
      x: {
        time: true,
        range: () => [xRange.min, xRange.max]
      },
      y: {
        range: getYRange
      }
    },
    hooks: {
      drawSeries: [],
      ready: [onReadyChart],
      setCursor: [setCursor],
      setSelect: [setSelect(setPlotScale)],
      destroy: [handleDestroy],
    },
    legend: { show: false },
    axes: getAxes([{}, { scale: "y" }]),
    tzDate: ts => uPlot.tzDate(new Date(Math.round(ts * 1000)), timezone),
  };

  return {
    options,
    series,
    focusDataIdx,
  };
};

export default useBarHitsOptions;
