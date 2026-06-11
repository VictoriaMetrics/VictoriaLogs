import { TimeParams, TimePeriod } from "../../types";
import { vmDate, VmDate, VmDateInstance } from "./vmDate";
import { nanosToIsoString, nanosecondsToSeconds } from "./nano";

export const MS_PER_SECOND = 1000;

export const secondsToMilliseconds = (seconds: number): number => {
  return seconds * MS_PER_SECOND;
};

export const millisecondsToSeconds = (milliseconds: number): number => {
  return milliseconds / MS_PER_SECOND;
};

export const toEpochSeconds = (ts: number | Date | VmDate | string): number => {
  if (typeof ts === "number") return ts;
  return nanosecondsToSeconds(vmDate(ts).nano().timestamp());
};

export const unixSecondsToDateTime = (ts: number): VmDateInstance => {
  return vmDate(secondsToMilliseconds(ts));
};

export const timePeriodToTimeParams = ({ from, to }: TimePeriod): TimeParams => {
  return {
    start: vmDate(from).nano().timestamp(),
    end: vmDate(to).nano().timestamp(),
  };
};

export const normalizeTimeParams = (period: TimeParams): TimeParams => {
  const isSameTime = period.start === period.end;
  if (!isSameTime) return period;

  return {
    start: period.start > 0n ? period.start - 1n : period.start,
    end: period.end + 1n,
  };
};

export const timeParamsToDateRange = ({ start, end }: TimeParams): TimePeriod => {
  return {
    from: nanosToIsoString(start),
    to: nanosToIsoString(end)
  };
};
