import { HitsTimeParams } from "../../../utils/logs";
import {
  getNanoTimestamp,
  getNanosecondsFromDuration,
  nanosToIsoString,
} from "../../../utils/time";

type HitsTimeRangesOptions = HitsTimeParams & {
  offset: string;
};

export const getHitsTimeRanges = ({ start, end, step, offset }: HitsTimeRangesOptions): HitsTimeParams[] => {
  const startNs = getNanoTimestamp(start, Date.parse(start));
  const endNs = getNanoTimestamp(end, Date.parse(end));
  const stepNs = getNanosecondsFromDuration(step);

  // getNanosecondsFromDuration only accepts positive values.
  const offsetValue = offset.replace(/^[+-]/, "");
  const offsetNs = getNanosecondsFromDuration(offsetValue) * (offset.startsWith("-") ? -1n : 1n);

  if (stepNs <= 0n) {
    throw new Error("Step must be greater than zero");
  }

  if (startNs >= endNs) return [];

  const alignToBarStart = (timestamp: bigint): bigint => {
    const shifted = timestamp + offsetNs;
    const remainder = ((shifted % stepNs) + stepNs) % stepNs;
    return shifted - remainder - offsetNs;
  };

  const firstBarStart = alignToBarStart(startNs);
  const lastBarStart = alignToBarStart(endNs - 1n);
  const ranges: HitsTimeParams[] = [];

  for (
    let barStart = lastBarStart;
    barStart >= firstBarStart;
    barStart -= stepNs
  ) {
    ranges.push({
      start: nanosToIsoString(barStart),
      end: nanosToIsoString(barStart + stepNs),
      step,
    });
  }

  return ranges;
};
