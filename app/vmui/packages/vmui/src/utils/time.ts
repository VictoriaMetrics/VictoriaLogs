import { RelativeTimeOption, TimeParams, TimePeriod, Timezone } from "../types";
import dayjs, { UnitTypeShort } from "dayjs";
import { DATE_ISO_FORMAT } from "../constants/date";
import timezones from "../constants/timezones";
import { getFromStorage } from "./storage";

export const limitsDurations = { min: 1, max: 1.578e+11 }; // min: 1 ms, max: 5 years

// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
const supportedValuesOf = Intl.supportedValuesOf;
export const supportedTimezones = supportedValuesOf ? supportedValuesOf("timeZone") as string[] : timezones;

// The list of supported units could be the following -
// https://prometheus.io/docs/prometheus/latest/querying/basics/#time-durations
export const supportedDurations = [
  { long: "years", short: "y", possible: "year" },
  { long: "weeks", short: "w", possible: "week" },
  { long: "days", short: "d", possible: "day" },
  { long: "hours", short: "h", possible: "hour" },
  { long: "minutes", short: "m", possible: "min" },
  { long: "seconds", short: "s", possible: "sec" },
  { long: "milliseconds", short: "ms", possible: "millisecond" }
];

const shortDurations = supportedDurations.map(d => d.short);

export const humanizeSeconds = (num: number): string => {
  return getDurationFromMilliseconds(dayjs.duration(num, "seconds").asMilliseconds());
};

export const isSupportedDuration = (str: string): Partial<Record<UnitTypeShort, string>> | undefined => {

  const digits = str.match(/\d+/g);
  const words = str.match(/[a-zA-Z]+/g);

  if (words && digits && shortDurations.includes(words[0])) {
    return { [words[0]]: digits[0] };
  }
};

export const getSecondsFromDuration = (dur: string) => {
  const shortSupportedDur = shortDurations.join("|");
  const regexp = new RegExp(`\\d+(\\.\\d+)?[${shortSupportedDur}]+`, "g");
  const durItems = dur.match(regexp) || [];

  const durObject = durItems.reduce((prev, curr) => {

    const dur = isSupportedDuration(curr);
    if (dur) {
      return {
        ...prev,
        ...dur
      };
    } else {
      return {
        ...prev
      };
    }
  }, {});

  return dayjs.duration(durObject).asSeconds();
};

export const getTimeperiodForDuration = (dur: string, date?: Date): TimeParams => {
  const end = date ? dayjs(date).unix() : dayjs().unix();
  const delta = getSecondsFromDuration(dur);

  return {
    start: end - delta,
    end: end,
  };
};

export const formatDateForNativeInput = (date: Date): string => {
  return dayjs.tz(date).format(DATE_ISO_FORMAT);
};

export const getDurationFromMilliseconds = (ms: number): string => {
  if (ms === 0) return "0ms";

  const d = dayjs.duration(ms);
  const units = [
    { val: Math.floor(d.asDays()), label: "d" },
    { val: d.hours(), label: "h" },
    { val: d.minutes(), label: "m" },
    { val: d.seconds(), label: "s" },
    { val: d.milliseconds(), label: "ms" },
  ];

  return units
    .filter(u => u.val > 0)
    .map(u => `${u.val}${u.label}`)
    .join("") || "0ms";
};

export const getDurationFromPeriod = (p: TimePeriod): string => {
  const ms = p.to.valueOf() - p.from.valueOf();
  return getDurationFromMilliseconds(ms);
};

export const dateFromSeconds = (epochTimeInSeconds: number): Date => {
  const date = dayjs(epochTimeInSeconds * 1000);
  return date.isValid() ? date.toDate() : new Date();
};

const getYesterday = () => dayjs().tz().subtract(1, "day").endOf("day").toDate();
const getToday = () => dayjs().tz().endOf("day").toDate();

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
  until: o.until ? o.until : () => dayjs().tz().toDate(),
  ...o
}));

export const getUTCByTimezone = (timezone: string) => {
  const date = dayjs().tz(timezone);
  return `UTC${date.format("Z")}`;
};

export const getTimezoneList = (search = "") => {
  const regexp = new RegExp(search, "i");

  return supportedTimezones.reduce((acc: {[key: string]: Timezone[]}, region) => {
    const zone = (region.match(/^(.*?)\//) || [])[1] || "unknown";
    const utc = getUTCByTimezone(region);
    const utcForSearch = utc.replace(/UTC|0/, "");
    const regionForSearch = region.replace(/[/_]/g, " ");
    const item = {
      region,
      utc,
      search: `${region} ${utc} ${regionForSearch} ${utcForSearch}`
    };
    const includeZone = !search || (search && regexp.test(item.search));

    if (includeZone && acc[zone]) {
      acc[zone].push(item);
    } else if (includeZone) {
      acc[zone] = [item];
    }

    return acc;
  }, {});
};

export const setTimezone = (timezone: string) => {
  dayjs.tz.setDefault(timezone);
};

const isValidTimezone = (timezone: string) => {
  try {
    dayjs().tz(timezone);
    return true;
  } catch (e) {
    return false;
  }
};

export const getBrowserTimezone = () => {
  const timezone = dayjs.tz.guess();
  const isValid = isValidTimezone(timezone);
  return  {
    isValid,
    title: isValid ? `Browser Time (${timezone})` : "Browser timezone (UTC)",
    region: isValid ? timezone : "UTC",
  };
};

export const getNanoTimestamp = (dateStr: string): bigint => {
  if (!dateStr) return 0n;

  // Get the millisecond timestamp using dayjs
  const baseMs = dayjs(dateStr).valueOf(); // milliseconds

  // If the date string doesn't contain a fractional part, return the timestamp in nanoseconds directly
  if (!dateStr.includes(".")) {
    return BigInt(baseMs) * 1000000n;
  }

  // Extract the fractional part between the decimal point and the "Z" character
  const match = dateStr.match(/\.(\d+)Z/);
  if (!match) {
    return BigInt(baseMs) * 1000000n;
  }

  let fraction = match[1];
  // Pad with trailing zeros to represent nanoseconds if necessary
  fraction = fraction.padEnd(9, "0");

  // The first 3 digits are already included in baseMs,
  // the remaining 6 digits represent additional nanoseconds
  const extraNano = parseInt(fraction.slice(3), 10);

  // Return the full timestamp in nanoseconds as a BigInt
  return BigInt(baseMs) * 1000000n + BigInt(extraNano);
};

export const toNanoPrecision = (time: string): string => {
  const match = time.match(/^(.+T\d{2}:\d{2}:\d{2})(\.(\d+))?Z$/);

  if (!match) {
    throw new Error("Invalid time format");
  }

  const base = match[1];
  const fraction = match[3] || "";
  const nanoFraction = (fraction + "000000000").slice(0, 9);

  return `${base}.${nanoFraction}Z`;
};

export const getPreviousRange = (period: TimeParams): TimeParams => {
  const { start, end } = period;
  const duration = end - start;
  const prevStart = start - duration;
  const prevEnd = end - duration;

  return {
    start: prevStart,
    end: prevEnd,
  };
};

// Formats a date-time string while respecting the current timezone.
// If the provided `format` includes millisecond tokens (e.g., .SSS), those will be replaced
// with a 9-digit fractional part from the input timestamp (padded to nanoseconds).
// If the format does not include .SSS, nanoseconds will NOT be appended.
// Example:
//  - input time: 2025-09-15T10:00:00.123456Z
//  - format: "YYYY-MM-DD HH:mm:ss.SSS"
//  - output (local tz): "2025-09-15 13:00:00.123456000" (depending on tz offset)
export const formatDateWithNanoseconds = (dateStr: string, format: string): string => {
  if (!dateStr) return "";

  const hasMillisToken = /\.SSS/.test(format);

  // If no millisecond token in format, just format normally without fractional seconds
  if (!hasMillisToken) {
    return dayjs(dateStr).tz().format(format);
  }

  // Derive a 9-digit fractional seconds string by reusing toNanoPrecision
  // Fall back to local extraction for non-Z inputs to avoid throwing
  let fraction: string;
  try {
    const normalized = toNanoPrecision(dateStr); // ...SSS.. to 9 digits, always ends with Z
    fraction = (normalized.match(/\.(\d{9})Z$/)?.[1]) || "000000000";
  } catch {
    const fracMatch = dateStr.match(/\.(\d+)(?:Z|[+-]\d{2}:?\d{2})?$/);
    fraction = (fracMatch?.[1] || "").padEnd(9, "0").slice(0, 9);
  }

  // Remove millisecond token from the format, since we'll append 9-digit fraction manually
  const baseFormat = format.replace(/\.SSS/g, "");

  // Format the base part (up to whole seconds) in the current/default timezone
  const base = dayjs(dateStr).tz().format(baseFormat);

  // Append nanoseconds only when .SSS is requested by the format
  return `${base}.${fraction}`;
};

export const toEpochSeconds = (ts: number | dayjs.Dayjs | string) => dayjs(ts).valueOf()/1000;

export const initTimezone = () => {
  const timezone = getFromStorage("TIMEZONE") as string || getBrowserTimezone().region;
  setTimezone(timezone);
  return timezone;
};

