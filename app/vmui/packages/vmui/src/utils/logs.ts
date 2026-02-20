import { TimeParams } from "../types";
import dayjs from "dayjs";
import { LOGS_GROUP_BY } from "../constants/logs";
import { LogHits, Logs } from "../api/types";
import { OTHER_HITS_LABEL } from "../components/Chart/BarHitsChart/hooks/useBarHitsOptions";

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

export const getHitsTimeParams = (period: TimeParams, bars: number) => {
  const start = dayjs(period.start * 1000);
  const end = dayjs(period.end * 1000);
  const totalMs = Math.max(1, end.diff(start, "ms"));

  const step = Math.max(1, Math.ceil(totalMs / bars));

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

export const isSameLog = (a: Logs, b: Logs): boolean => {
  if (a._time !== b._time) return false;
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  return [...keys].every(k => a[k] === b[k]);
};

export const removeExactLog = (logs: Logs[], target: Logs): Logs[] => {
  return logs.filter(log => !isSameLog(log, target));
};
