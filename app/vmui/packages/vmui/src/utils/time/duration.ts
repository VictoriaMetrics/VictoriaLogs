// The list of supported units could be the following -
// https://prometheus.io/docs/prometheus/latest/querying/basics/#float-literals-and-time-durations
import { TimePeriod } from "../../types";
import { vmDate } from "./vmDate";
import { absNanoseconds, NANOSECONDS_PER_MILLISECOND, nanosecondsToMilliseconds } from "./nano";
import { timePeriodToTimeParams } from "./convert";

const DURATION_UNITS_IN_NANOS = {
  ns: 1n,
  ms: 1_000_000n,
  s: 1_000_000_000n,
  m: 60n * 1_000_000_000n,
  h: 60n * 60n * 1_000_000_000n,
  d: 24n * 60n * 60n * 1_000_000_000n,
  w: 7n * 24n * 60n * 60n * 1_000_000_000n,
  y: 365n * 24n * 60n * 60n * 1_000_000_000n,
} as const;

export const getNanosecondsFromDuration = (dur: string): bigint => {
  const fullRegexp = /^(?:\d+(?:\.\d+)?(?:ns|ms|y|w|d|h|m|s))+$/;

  if (!fullRegexp.test(dur)) {
    throw new Error(`Invalid duration: "${dur}"`);
  }

  const regexp = /(\d+(?:\.\d+)?)(ns|ms|y|w|d|h|m|s)/g;

  return Array.from(dur.matchAll(regexp)).reduce((total, [, value, unit]) => {
    const [intPart, fractionPart = ""] = value.split(".");
    const unitNanos = DURATION_UNITS_IN_NANOS[unit as keyof typeof DURATION_UNITS_IN_NANOS];

    const whole = BigInt(intPart) * unitNanos;
    const fraction = fractionPart
      ? (BigInt(fractionPart) * unitNanos) / (10n ** BigInt(fractionPart.length))
      : 0n;

    return total + whole + fraction;
  }, 0n);
};

export const getMillisecondsFromDuration = (dur: string): number => {
  const nanos = getNanosecondsFromDuration(dur);
  return nanosecondsToMilliseconds(nanos);
};

export const getDurationFromMilliseconds = (ms: number): string => {
  if (ms === 0) return "0ms";

  const d = vmDate.duration(ms);
  const milliseconds = d.milliseconds();

  const units = [
    { val: Math.floor(d.asDays()), label: "d" },
    { val: d.hours(), label: "h" },
    { val: d.minutes(), label: "m" },
    { val: d.seconds(), label: "s" },
    { val: milliseconds, label: "ms" },
  ];

  return units
    .filter(u => u.val > 0)
    .map(u => `${Number.isInteger(u.val) ? u.val : Number(u.val.toFixed(6))}${u.label}`)
    .join("") || "0ms";
};

export const getDurationFromNanoseconds = (ns: bigint): string => {
  if (ns === 0n) return "0ms";

  const absNs = absNanoseconds(ns);

  if (absNs < NANOSECONDS_PER_MILLISECOND) {
    return `${Number(absNs) / Number(NANOSECONDS_PER_MILLISECOND)}ms`;
  }

  const ms = Number(absNs) / Number(NANOSECONDS_PER_MILLISECOND);

  return getDurationFromMilliseconds(ms);
};

export const getDurationFromPeriod = (p: TimePeriod): string => {
  const { start, end } = timePeriodToTimeParams(p);

  return getDurationFromNanoseconds(end - start);
};

export function formatRequestDuration(ms: number): string {
  if (ms < 1000) {
    return `${Math.max(1, Math.round(ms))}ms`;
  }

  return `${(ms / 1000).toFixed(1)}s`;
}

