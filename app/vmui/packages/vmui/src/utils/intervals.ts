import { IntervalOption, TimeParams } from "../types";
import { ALLOWED_INTERVALS } from "../constants/intervals";
import { LOGS_BAR_COUNT_DEFAULT, LOGS_INTERVALS_COUNT } from "../constants/logs";

/**
 * Input: start/end in UNIX nanoseconds.
 * Output: interval options around the target chart step.
 * Returns [] for invalid or zero-length ranges.
 */
export function getIntervalOptions({ start, end }: TimeParams): IntervalOption[] {
  const rangeNs = end - start;

  if (rangeNs <= 0n) return [];

  const targetNs = rangeNs / BigInt(LOGS_BAR_COUNT_DEFAULT);
  const targetIndex = findNearestIntervalIndex(targetNs);
  return getIntervalsAround(targetIndex, LOGS_INTERVALS_COUNT);
}

export function getDefaultIntervalOption({ start, end }: TimeParams): IntervalOption | undefined {
  const rangeNs = end - start;

  if (rangeNs <= 0n) return undefined;

  const targetNs = rangeNs / BigInt(LOGS_BAR_COUNT_DEFAULT);
  const index = findNearestIntervalIndex(targetNs);
  return ALLOWED_INTERVALS[index];
}

const findUpperIndex = (targetNs: bigint): number => {
  const index = ALLOWED_INTERVALS.findIndex(({ valueNs }) => valueNs >= targetNs);
  return index === -1 ? ALLOWED_INTERVALS.length : index;
};

const findNearestIntervalIndex = (targetNs: bigint): number => {
  if (targetNs <= ALLOWED_INTERVALS[0].valueNs) return 0;

  const lastIndex = ALLOWED_INTERVALS.length - 1;
  if (targetNs >= ALLOWED_INTERVALS[lastIndex].valueNs) return lastIndex;

  const upperIndex = findUpperIndex(targetNs);
  const lowerIndex = upperIndex - 1;

  const lowerValueNs = ALLOWED_INTERVALS[lowerIndex].valueNs;
  const upperValueNs = ALLOWED_INTERVALS[upperIndex].valueNs;

  const distanceToLower = Math.abs(Math.log(Number(lowerValueNs) / Number(targetNs)));
  const distanceToUpper = Math.abs(Math.log(Number(upperValueNs) / Number(targetNs)));

  return distanceToLower < distanceToUpper ? lowerIndex : upperIndex;
};

const getIntervalsAround = (index: number, count: number): IntervalOption[] => {
  const half = Math.floor(count / 2);
  const proposedStart = index - half;
  const maxStart = Math.max(0, ALLOWED_INTERVALS.length - count);
  const start = Math.min(Math.max(0, proposedStart), maxStart);
  return ALLOWED_INTERVALS.slice(start, start + count);
};
