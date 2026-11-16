import {
  nanosToIsoString,
  secondsToNanoseconds,
  vmDate, VmDateInstance,
} from "../../../../utils/time";
import { DATE_FORMAT } from "../../../../constants/date";

type TooltipTimePrecision = "date" | "minute" | "second" | "millisecond" | "nano";

const SECONDS_PER_MILLISECOND = 0.001;
const SECONDS_PER_MINUTE = 60;
const SECONDS_PER_DAY = 24 * 60 * 60;

const isMultipleOf = (value: number, unit: number) => {
  const remainder = value % unit;
  return Math.abs(remainder) < Number.EPSILON || Math.abs(remainder - unit) < Number.EPSILON;
};

const getTooltipTimePrecision = (stepSeconds: number): TooltipTimePrecision => {
  const step = Math.abs(stepSeconds);

  if (!Number.isFinite(step) || step <= 0) return "nano";

  if (step >= SECONDS_PER_DAY && isMultipleOf(step, SECONDS_PER_DAY)) return "date";
  if (step >= SECONDS_PER_MINUTE && isMultipleOf(step, SECONDS_PER_MINUTE)) return "minute";
  if (step >= 1) return "second";
  if (step >= SECONDS_PER_MILLISECOND) return "millisecond";

  return "nano";
};

const toVmDate = (ts: number) => {
  if (!Number.isFinite(ts)) return null;

  const iso = nanosToIsoString(secondsToNanoseconds(ts));
  return vmDate(iso);
};

const formatTime = (
  date: VmDateInstance | null,
  precision: TooltipTimePrecision
): string => {
  if (!date) return "";

  switch (precision) {
    case "minute":
      return date.tz().format("HH:mm");

    case "second":
      return date.tz().format("HH:mm:ss");

    case "millisecond":
      return date.tz().format("HH:mm:ss.SSS");

    case "nano":
      return date.nano().format("HH:mm:ss.SSS");

    default:
      return "";
  }
};

export const getTooltipTimeRangeLines = (
  start: number,
  end: number,
  step: number,
): string => {
  const precision = getTooltipTimePrecision(step);

  const startParts = [];
  const endParts = [];

  const startDate = toVmDate(start);
  if (startDate) {
    const startDay = startDate.tz().format(DATE_FORMAT) || "";
    startParts.push(startDay);

    const startTime = formatTime(startDate, precision);
    startParts.push(startTime);
  }

  const endDate = toVmDate(end);

  if (endDate) {
    const endDay = endDate.tz().format(DATE_FORMAT) || "";
    const isSameDay = startParts[0] === endDay;
    !isSameDay && endParts.push(endDay);

    const endTime = formatTime(endDate, precision);
    endParts.push(endTime);
  }

  const startStr = startParts.filter(Boolean).join(" ");
  const endStr = endParts.filter(Boolean).join(" ");

  return [startStr, endStr].filter(Boolean).join(" - ");
};
