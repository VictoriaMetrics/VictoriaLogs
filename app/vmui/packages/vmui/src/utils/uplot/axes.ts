import uPlot, { Axis, Series } from "uplot";
import { formatTicks, getTextWidth } from "./helpers";
import { getCssVariable } from "../theme";
import { AxisExtend } from "../../types";

// see https://github.com/leeoniya/uPlot/tree/master/docs#axis--grid-opts
const timeValues = [
  // tick incr      default           year                            month day                      hour  min            sec   mode
  [3600 * 24 * 365, "{YYYY}",         null,                           null, null,                    null, null,          null, 1],
  [3600 * 24 * 28,  "{MMM}",          "\n{YYYY}",                     null, null,                    null, null,          null, 1],
  [3600 * 24,       "{MM}-{DD}",      "\n{YYYY}",                     null, null,                    null, null,          null, 1],
  [3600,            "{HH}:{mm}",      "\n{YYYY}-{MM}-{DD}",           null, "\n{MM}-{DD}",           null, null,          null, 1],
  [60,              "{HH}:{mm}",      "\n{YYYY}-{MM}-{DD}",           null, "\n{MM}-{DD}",           null, null,          null, 1],
  [1,               "{HH}:{mm}:{ss}", "\n{YYYY}-{MM}-{DD}",           null, "\n{MM}-{DD} {HH}:{mm}", null, null,          null, 1],
  [0.001,           ":{ss}.{fff}",    "\n{YYYY}-{MM}-{DD} {HH}:{mm}", null, "\n{MM}-{DD} {HH}:{mm}", null, "\n{HH}:{mm}", null, 1],
];

export const getAxes = (series: Series[], unit?: string): Axis[] => Array.from(new Set(series.map(s => s.scale))).map(a => {
  const font = "10px Arial";
  const stroke = getCssVariable("color-text");
  const axis = {
    scale: a,
    show: true,
    size: sizeAxis,
    stroke,
    font,
    values: (u: uPlot, ticks: number[]) => formatTicks(u, ticks, unit)
  };
  if (!a) return { space: 80, values: timeValues, stroke, font };
  if (!(Number(a) % 2) && a !== "y") return { ...axis, side: 1 };
  return axis;
});

export const getMinMaxBuffer = (min: number | null, max: number | null): [number, number] => {
  if (min == null || max == null) {
    return [-1, 1];
  }
  const valueRange = Math.abs(max - min) || Math.abs(min) || 1;
  const padding = 0.02*valueRange;
  return [min - padding, max + padding];
};

export const sizeAxis = (u: uPlot, values: string[], axisIdx: number, cycleNum: number): number => {
  const axis = u.axes[axisIdx] as AxisExtend;

  if (cycleNum > 1) return axis._size || 60;

  let axisSize = 6 + (axis?.ticks?.size || 0) + (axis.gap || 0);

  const longestVal = (values ?? []).reduce((acc, val) => val?.length > acc.length ? val : acc, "");
  if (longestVal != "") axisSize += getTextWidth(longestVal, "10px Arial");

  return Math.ceil(axisSize);
};
