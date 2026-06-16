import { TimeParams } from "../types";
import { LOGS_BAR_COUNT_DEFAULT, LOGS_GROUP_BY } from "../constants/logs";
import { LogHits, Logs } from "../api/types";
import { OTHER_HITS_LABEL } from "../components/Chart/BarHitsChart/hooks/useBarHitsOptions";
import { nanosecondsToMilliseconds, nanosToIsoString } from "./time";

export const getStreamPairs = (stream: string): string[] => {
  const s = stream.trim();

  // Only treat as {...} if it is actually wrapped and can be empty (`{}`)
  if (!(s.startsWith("{") && s.endsWith("}"))) return [s].filter(Boolean);

  const inner = s.slice(1, -1).trim();
  if (!inner) return [];

  const out: string[] = [];
  let buf = "";
  let inQuotes = false;
  let escaped = false;

  // Split by commas, but ignore commas inside quoted values
  for (let i = 0; i < inner.length; i++) {
    const ch = inner[i];

    if (escaped) {
      buf += ch;
      escaped = false;
      continue;
    }

    if (ch === "\\") {
      buf += ch;
      escaped = true;
      continue;
    }

    if (ch === "\"") {
      buf += ch;
      inQuotes = !inQuotes;
      continue;
    }

    if (ch === "," && !inQuotes) {
      const token = buf.trim();
      if (token) out.push(token);
      buf = "";
      continue;
    }

    buf += ch;
  }

  const last = buf.trim();
  if (last) out.push(last);

  return out;
};

export const getStreamKeys = (stream: string): string[] => {
  return getStreamPairs(stream)
    .map(p => p.trim())
    .map(p => p.split("=", 1)[0]?.trim())
    .filter(Boolean);
};

export const getAllStreamKeys = (data: Array<Record<string, unknown>>): string[] => {
  const keys = new Set<string>();
  const n = data.length;

  for (let i = 0; i < n; i++) {
    const stream = data[i]?._stream;
    if (typeof stream !== "string") continue;

    for (const k of getStreamKeys(stream)) keys.add(k);
  }

  return [...keys];
};

type HitsTimeParams = {
  /** ISO 8601 string with up to nanosecond precision, e.g. `"2026-06-01T12:00:24.414146743Z"` */
  start: string;
  /** ISO 8601 string with up to nanosecond precision, e.g. `"2026-06-01T12:00:24.414146743Z"` */
  end: string;
  /** Step size in milliseconds. */
  step: number;
}

export const getHitsTimeParams = (period: TimeParams): HitsTimeParams => {
  const start = nanosToIsoString(period.start);
  const end = nanosToIsoString(period.end);

  const totalMs = Math.max(1, nanosecondsToMilliseconds(period.end - period.start));
  const step = totalMs / LOGS_BAR_COUNT_DEFAULT;

  return { start, end, step };
};

export const convertToFieldFilter = (value: string, field = LOGS_GROUP_BY) => {
  const isKeyValue = /(.+)?=(".+")/.test(value);

  if (isKeyValue) {
    return value.replace(/=/, ": ");
  }

  // Escape double quotes in the field value
  return `${field}: ${JSON.stringify(value)}`;
};

export const calculateTotalHits = (hits: LogHits[]): number => {
  return hits.reduce((acc, item) => acc + (item.total || 0), 0);
};

export const sortLogHits = <T extends { label?: string }>(key: keyof T) => (a: T, b: T): number => {
  if (a.label === OTHER_HITS_LABEL) return 1;
  if (b.label === OTHER_HITS_LABEL) return -1;

  const aValue = a[key] as unknown as number;
  const bValue = b[key] as unknown as number;

  return bValue - aValue;
};

export const isEqualLogByKeys = (a: Logs, b: Logs, keys: Array<keyof Logs>): boolean => {
  return keys.every(key => a[key] === b[key]);
};

export const removeLogsByKeys = (logs: Logs[], target: Logs, keys: Array<keyof Logs>): Logs[] => {
  return logs.filter(log => !isEqualLogByKeys(log, target, keys));
};
