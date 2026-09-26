import { RelativeTimeOption, TimeParams } from "../../types";
import { getNanosecondsFromDuration } from "./duration";
import { normalizeTimeParams } from "./convert";
import { vmDate } from "./vmDate";

const getYesterday = () => vmDate().tz().subtract(1, "day").endOf("day").toISOString();
const getToday = () => vmDate().tz().endOf("day").toISOString();

export const relativeTimeOptions: RelativeTimeOption[] = [
  { title: "Last 5 minutes", duration: "5m", isDefault: true },
  { title: "Last 15 minutes", duration: "15m" },
  { title: "Last 30 minutes", duration: "30m", },
  { title: "Last 1 hour", duration: "1h" },
  { title: "Last 3 hours", duration: "3h" },
  { title: "Last 6 hours", duration: "6h" },
  { title: "Last 12 hours", duration: "12h" },
  { title: "Last 24 hours", duration: "24h" },
  { title: "Last 2 days", duration: "2d" },
  { title: "Last 7 days", duration: "7d" },
  { title: "Last 30 days", duration: "30d" },
  { title: "Last 90 days", duration: "90d" },
  { title: "Last 180 days", duration: "180d" },
  { title: "Last 1 year", duration: "1y" },
  { title: "Yesterday", duration: "1d", until: getYesterday },
  { title: "Today", duration: "1d", until: getToday },
].map(o => ({
  id: o.title.replace(/\s/g, "_").toLocaleLowerCase(),
  until: o.until ? o.until : () => vmDate().tz().toISOString(),
  ...o
}));

/**
 * @param dur - Duration string.
 * @param date - ISO 8601 string with up to nanosecond precision.
 */
export const getTimeParamsForDuration = (dur: string, date: string): TimeParams => {
  const end = vmDate(date).nano().timestamp();
  const delta = getNanosecondsFromDuration(dur);

  return normalizeTimeParams({
    start: end - delta,
    end,
  });
};

export const getPreviousRange = (period: TimeParams): TimeParams => {
  const { start, end } = period;
  const duration = end - start;
  const prevStart = start - duration;
  const prevEnd = end - duration;

  return normalizeTimeParams({
    start: prevStart,
    end: prevEnd,
  });
};
