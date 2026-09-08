const NANO_FRACTION_DIGITS = 9;
const MILLISECOND_FRACTION_DIGITS = 3;
export const NANOSECONDS_PER_MILLISECOND = 1_000_000n;
export const NANOSECONDS_PER_SECOND = 1_000_000_000n;
const ISO_BASE_RE = /^(.+T\d{2}:\d{2}:\d{2})(?:\.\d+)?Z$/;
const FRACTIONAL_SECONDS_RE = /\.(\d+)(?=Z$|[+-]\d{2}:?\d{2}$|$)/;

export const secondsToNanoseconds = (seconds: number): bigint => {
  if (!Number.isFinite(seconds)) {
    throw new RangeError("seconds must be finite");
  }

  if (seconds < 0) {
    throw new RangeError("seconds must be non-negative");
  }

  const text = String(seconds);

  if (/[eE]/.test(text)) {
    const nanos = Math.trunc(seconds * 1e9);

    if (!Number.isSafeInteger(nanos)) {
      throw new RangeError("seconds is too large to convert safely to nanoseconds");
    }

    return BigInt(nanos);
  }

  const [whole, fraction = ""] = text.split(".");
  const nanoFraction = fraction.padEnd(9, "0").slice(0, 9);

  return BigInt(whole) * NANOSECONDS_PER_SECOND + BigInt(nanoFraction);
};

export const nanosecondsToSeconds = (nanos: bigint): number => {
  const seconds = nanos / NANOSECONDS_PER_SECOND;
  const fraction = nanos % NANOSECONDS_PER_SECOND;

  return Number(seconds) + Number(fraction) / Number(NANOSECONDS_PER_SECOND);
};

export const nanosecondsToMilliseconds = (nanoseconds: bigint): number => {
  return Number(nanoseconds) / Number(NANOSECONDS_PER_MILLISECOND);
};

/**
 * Normalizes a fractional seconds string to nanosecond precision by padding or trimming it to 9 digits.
 *
 * @param fraction - Fractional seconds string.
 * @returns Fractional seconds string with exactly 9 digits.
 */
const normalizeNanoFraction = (fraction = ""): string => {
  return fraction.padEnd(NANO_FRACTION_DIGITS, "0").slice(0, NANO_FRACTION_DIGITS);
};

/**
 * Extracts fractional seconds from a date string and normalizes them to nanosecond precision.
 *
 * @param dateStr - Date/time string that may contain fractional seconds.
 * @returns Fractional seconds string with exactly 9 digits.
 */
const getNanoFraction = (dateStr: string): string => {
  const fraction = dateStr.match(FRACTIONAL_SECONDS_RE)?.[1];
  return normalizeNanoFraction(fraction);
};

/**
 * Converts an ISO timestamp with a `Z` timezone suffix to nanosecond precision.
 *
 * @param baseIso - ISO timestamp ending with `Z`.
 * @param source - Date/time string used to extract fractional seconds.
 * @returns ISO timestamp with exactly 9 fractional second digits.
 * @throws If the input does not match the expected timestamp format.
 */
export const toNanoPrecision = (baseIso: string, source = baseIso): string => {
  const match = baseIso.match(ISO_BASE_RE);

  if (!match) {
    throw new Error("Invalid time format");
  }

  const base = match[1];
  const nanoFraction = getNanoFraction(source);

  return `${base}.${nanoFraction}Z`;
};

/**
 * Builds a nanosecond timestamp from a base millisecond timestamp and the extra nanoseconds from a date string.
 *
 * @param dateStr - Date/time string that may contain fractional seconds.
 * @param baseMs - Base timestamp in milliseconds.
 * @returns Timestamp in nanoseconds as a bigint, or `0n` when `dateStr` is empty.
 */
export const getNanoTimestamp = (dateStr: string, baseMs: number): bigint => {
  if (!dateStr) return 0n;

  const fraction = getNanoFraction(dateStr);
  const extraNano = BigInt(fraction.slice(MILLISECOND_FRACTION_DIGITS) || "0");

  return BigInt(baseMs) * NANOSECONDS_PER_MILLISECOND + extraNano;
};

/**
 * Formats a date while preserving nanosecond precision when the format includes `.SSS`.
 *
 * @param dateStr - Original date/time string that may contain fractional seconds.
 * @param format - Date format string.
 * @param formatBaseDate - Function used to format the base date.
 * @returns Formatted date string with nanosecond precision when requested, otherwise the base formatted date.
 */
export const formatDateWithNanoseconds = (
  dateStr: string,
  format: string,
  formatBaseDate: (format: string) => string,
): string => {
  if (!dateStr) return "";

  if (!/\.SSS/.test(format)) {
    return formatBaseDate(format);
  }

  const fraction = getNanoFraction(dateStr);
  const baseFormat = format.replace(/\.SSS/g, "");
  const base = formatBaseDate(baseFormat);

  return `${base}.${fraction}`;
};

/**
 * Converts a non-negative nanosecond timestamp to an ISO string with 9 fractional second digits.
 *
 * @param nanos - Timestamp in nanoseconds as a bigint. Negative values are not supported.
 * @returns ISO timestamp with nanosecond precision, e.g. `"2026-06-01T12:00:24.414146743Z"`.
 */
export const nanosToIsoString = (nanos: bigint): string => {
  let ms = nanos / NANOSECONDS_PER_MILLISECOND;
  let remainingNanos = nanos % NANOSECONDS_PER_MILLISECOND;

  if (remainingNanos < 0n) {
    ms -= 1n;
    remainingNanos += NANOSECONDS_PER_MILLISECOND;
  }

  const date = new Date(Number(ms));

  if (Number.isNaN(date.getTime())) {
    throw new Error("Invalid timestamp");
  }

  const base = date.toISOString().replace(/\.\d{3}Z$/, "");
  const millis = String(date.getUTCMilliseconds()).padStart(3, "0");
  const subMillis = String(remainingNanos).padStart(6, "0");

  return `${base}.${millis}${subMillis}Z`;
};

export const absNanoseconds = (value: bigint): bigint => {
  return value < 0n ? -value : value;
};
