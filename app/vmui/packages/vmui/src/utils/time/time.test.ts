import { describe, expect, it } from "vitest";
import {
  absNanoseconds,
  getDurationFromMilliseconds,
  getDurationFromNanoseconds,
  getDurationFromPeriod,
  getNanosecondsFromDuration,
  getDefaultTimezoneOffsetMinutes,
  getNowInTimezone,
  getTimeParamsForDuration,
  getUTCByTimezone,
  isValidDate,
  millisecondsToSeconds,
  nanosToIsoString,
  nanosecondsToMilliseconds,
  normalizeTimeParams,
  secondsToMilliseconds,
  secondsToNanoseconds,
  setTimezone,
  timeParamsToDateRange,
  timePeriodToTimeParams,
  nanosecondsToSeconds,
  toEpochSeconds,
  unixSecondsToDateTime,
  vmDate
} from "./index";

describe("Time utils", () => {
  describe("time unit conversions", () => {
    it("keeps numeric input as unix seconds", () => {
      expect(toEpochSeconds(1710000000)).toBe(1710000000);
    });

    it("converts date-like input to epoch seconds", () => {
      expect(toEpochSeconds("2024-03-09T16:00:00.250Z")).toBe(1710000000.25);
    });

    it("converts unix seconds to Date and vmDate instances", () => {
      expect(unixSecondsToDateTime(1710000000.25).toISOString()).toBe("2024-03-09T16:00:00.250Z");
    });

    it("converts between seconds and milliseconds", () => {
      expect(secondsToMilliseconds(1.5)).toBe(1500);
      expect(millisecondsToSeconds(1500)).toBe(1.5);
    });
  });

  describe("getDurationFromMilliseconds", () => {
    it("should return '0ms' for input 0", () => {
      expect(getDurationFromMilliseconds(0)).toBe("0ms");
    });

    it("should correctly format milliseconds only", () => {
      expect(getDurationFromMilliseconds(450)).toBe("450ms");
    });

    it("should correctly format seconds and milliseconds", () => {
      expect(getDurationFromMilliseconds(1250)).toBe("1s250ms");
    });

    it("should correctly format minutes, seconds, and milliseconds", () => {
      expect(getDurationFromMilliseconds(61500)).toBe("1m1s500ms");
    });

    it("should correctly format hours, minutes, seconds, and milliseconds", () => {
      expect(getDurationFromMilliseconds(3661500)).toBe("1h1m1s500ms");
    });

    it("should correctly format days, hours, minutes, seconds, and milliseconds", () => {
      expect(getDurationFromMilliseconds(90061000)).toBe("1d1h1m1s");
    });

    it("should skip zero units in the output", () => {
      expect(getDurationFromMilliseconds(3600000)).toBe("1h");
    });
  });

  describe("duration conversion with nanosecond precision", () => {
    it("converts supported duration strings to seconds", () => {
      expect(nanosecondsToSeconds(getNanosecondsFromDuration("1h30m250ms"))).toBe(5400.25);
    });

    it("converts duration strings to nanoseconds", () => {
      expect(getNanosecondsFromDuration("1.5s250ms3ns")).toBe(1_750_000_003n);
    });

    it("formats nanosecond durations, including sub-millisecond values", () => {
      expect(getDurationFromNanoseconds(0n)).toBe("0ms");
      expect(getDurationFromNanoseconds(123456n)).toBe("0.123456ms");
      expect(getDurationFromNanoseconds(-123456n)).toBe("0.123456ms");
      expect(getDurationFromNanoseconds(1_500_000_000n)).toBe("1s500ms");
    });

    it("calculates duration from an ISO nanosecond period", () => {
      expect(getDurationFromPeriod({
        from: "2024-03-09T16:00:00.000000100Z",
        to: "2024-03-09T16:00:00.000000900Z"
      })).toBe("0.0008ms");
    });
  });

  describe("nanosecond conversions", () => {
    it("converts seconds to nanoseconds and back", () => {
      const nanos = secondsToNanoseconds(1.23456789);

      expect(nanos).toBe(1_234_567_890n);
      expect(nanosecondsToSeconds(nanos)).toBeCloseTo(1.23456789);
    });

    it("keeps a full 9-digit fractional seconds part", () => {
      expect(secondsToNanoseconds(1.123456789)).toBe(1_123_456_789n);
      expect(secondsToNanoseconds(1.000000123)).toBe(1_000_000_123n);
    });

    it("does not add floating-point artifacts when converting decimal seconds", () => {
      const nanos = secondsToNanoseconds(1780578429.5146);

      expect(nanos).toBe(1_780_578_429_514_600_000n);
      expect(nanosToIsoString(nanos)).toBe("2026-06-04T13:07:09.514600000Z");
    });

    it("rejects non-finite seconds", () => {
      expect(() => secondsToNanoseconds(Number.POSITIVE_INFINITY)).toThrow("seconds must be finite");
    });

    it("rejects negative seconds", () => {
      expect(() => secondsToNanoseconds(-1)).toThrow("seconds must be non-negative");
    });

    it("converts nanoseconds to milliseconds", () => {
      expect(nanosecondsToMilliseconds(1_500_000n)).toBe(1.5);
    });

    it("serializes nanosecond timestamps to ISO strings", () => {
      expect(nanosToIsoString(1_710_000_000_250_123_456n)).toBe("2024-03-09T16:00:00.250123456Z");
    });

    it("returns absolute nanoseconds", () => {
      expect(absNanoseconds(-42n)).toBe(42n);
      expect(absNanoseconds(42n)).toBe(42n);
    });

    it("handles small seconds values serialized in scientific notation", () => {
      expect(secondsToNanoseconds(1e-7)).toBe(100n);
      expect(secondsToNanoseconds(1e-8)).toBe(10n);
      expect(secondsToNanoseconds(1e-9)).toBe(1n);
    });

    it("rejects scientific notation values that cannot be safely converted to nanoseconds", () => {
      expect(() => secondsToNanoseconds(1e21)).toThrow(RangeError);
    });
  });

  describe("time ranges", () => {
    it("converts TimeParams to an ISO nanosecond range", () => {
      expect(timeParamsToDateRange({
        start: 1_710_000_000_250_123_456n,
        end: 1_710_000_001_250_123_456n
      })).toEqual({
        from: "2024-03-09T16:00:00.250123456Z",
        to: "2024-03-09T16:00:01.250123456Z"
      });
    });

    it("converts TimePeriod to nanosecond TimeParams", () => {
      expect(timePeriodToTimeParams({
        from: "2024-03-09T16:00:00.250123456Z",
        to: "2024-03-09T16:00:01.250123456Z"
      })).toEqual({
        start: 1_710_000_000_250_123_456n,
        end: 1_710_000_001_250_123_456n
      });
    });

    it("builds TimeParams from a duration and end date", () => {
      expect(getTimeParamsForDuration("1.5s", "2024-03-09T16:00:00.250123456Z")).toEqual({
        start: 1_709_999_998_750_123_456n,
        end: 1_710_000_000_250_123_456n,
      });
    });

    it("expands zero-width periods by nanoseconds", () => {
      expect(normalizeTimeParams({ start: 10n, end: 10n })).toEqual({
        start: 9n,
        end: 11n
      });
      expect(normalizeTimeParams({ start: 0n, end: 0n })).toEqual({
        start: 0n,
        end: 1n
      });
      expect(getTimeParamsForDuration("0ms", "2024-03-09T16:00:00.250123456Z")).toEqual({
        start: 1_710_000_000_250_123_455n,
        end: 1_710_000_000_250_123_457n,
      });
    });
  });

  describe("date validation", () => {
    it("validates date-like values", () => {
      expect(isValidDate("2024-03-09T16:00:00.250Z")).toBe(true);
      expect(isValidDate("invalid-date")).toBe(false);
    });
  });

  describe("timezone helpers", () => {
    it("sets and reads UTC timezone deterministically", () => {
      setTimezone("UTC");

      try {
        expect(getUTCByTimezone("UTC")).toBe("UTC+00:00");
        expect(getNowInTimezone().format("Z")).toBe("+00:00");
        expect(getDefaultTimezoneOffsetMinutes()).toBe(0);
      } finally {
        vmDate.tz.setDefault();
      }
    });
  });

  describe("getNanoTimestamp", () => {
    it("should return 0n for an empty string", () => {
      expect(vmDate("").nano().timestamp()).toBe(0n);
    });

    it("correctly handles a date without a fractional part", () => {
      const dateStr = "2023-03-20T12:34:56Z";
      const baseMs = vmDate(dateStr).valueOf();
      const expected = BigInt(baseMs) * 1000000n;
      expect(vmDate(dateStr).nano().timestamp()).toBe(expected);
    });

    it("correctly handles a date with a fractional part of 3 digits", () => {
      // In this case, the fractional part "123" is padded to "123000000",
      // and the remaining part after the first 3 digits is "000000"
      const dateStr = "2023-03-20T12:34:56.123Z";
      const baseMs = vmDate(dateStr).valueOf();
      const expected = BigInt(baseMs) * 1000000n; // extraNano = 0
      expect(vmDate(dateStr).nano().timestamp()).toBe(expected);
    });

    it("correctly computes additional nanoseconds for a fractional part with more than 3 digits", () => {
      // For "123456", the fractional part is padded to "123456000",
      // extraNano = parseInt("456000") = 456000
      const dateStr = "2023-03-20T12:34:56.123456Z";
      const baseMs = vmDate(dateStr).valueOf();
      const extraNano = 456000n;
      const expected = BigInt(baseMs) * 1000000n + extraNano;
      expect(vmDate(dateStr).nano().timestamp()).toBe(expected);
    });

    it("correctly handles a date with a fractional part of 9 digits", () => {
      // For "123456789", extraNano = parseInt("456789") = 456789
      const dateStr = "2023-03-20T12:34:56.123456789Z";
      const baseMs = vmDate(dateStr).valueOf();
      const extraNano = 456789n;
      const expected = BigInt(baseMs) * 1000000n + extraNano;
      expect(vmDate(dateStr).nano().timestamp()).toBe(expected);
    });

    it("trims fractional parts longer than nanoseconds", () => {
      const dateStr = "2023-03-20T12:34:56.1234567891Z";
      const baseMs = vmDate(dateStr).valueOf();
      const extraNano = 456789n;
      const expected = BigInt(baseMs) * 1000000n + extraNano;
      expect(vmDate(dateStr).nano().timestamp()).toBe(expected);
    });

    it("preserves sub-millisecond precision for offset timestamps", () => {
      const dateStr = "2025-09-15T10:00:00.123456+02:00";
      const baseMs = vmDate(dateStr).valueOf();
      const extraNano = 456000n;
      const expected = BigInt(baseMs) * 1000000n + extraNano;
      expect(vmDate(dateStr).nano().timestamp()).toBe(expected);
    });

    it("returns the default value if the regex does not match (e.g., missing \"Z\")", () => {
      const dateStr = "2023-03-20T12:34:56.123";
      const baseMs = vmDate(dateStr).valueOf();
      const expected = BigInt(baseMs) * 1000000n;
      expect(vmDate(dateStr).nano().timestamp()).toBe(expected);
    });
  });

  describe("toNanoPrecision", () => {
    it("should pad fraction to 9 digits (microseconds -> nanoseconds)", () => {
      const input = "2024-09-19T14:41:13.76572Z";
      const expected = "2024-09-19T14:41:13.765720000Z";
      expect(vmDate(input).nano().toISOString()).toBe(expected);
    });

    it("should leave already correct 9-digit fraction untouched", () => {
      const input = "2024-09-19T14:41:13.123456789Z";
      const expected = "2024-09-19T14:41:13.123456789Z";
      expect(vmDate(input).nano().toISOString()).toBe(expected);
    });

    it("should pad shorter fractions (milliseconds -> nanoseconds)", () => {
      const input = "2024-09-19T14:41:13.123Z";
      const expected = "2024-09-19T14:41:13.123000000Z";
      expect(vmDate(input).nano().toISOString()).toBe(expected);
    });

    it("should add .000000000 if no fraction is present", () => {
      const input = "2024-09-19T14:41:13Z";
      const expected = "2024-09-19T14:41:13.000000000Z";
      expect(vmDate(input).nano().toISOString()).toBe(expected);
    });

    it("should throw error on invalid format", () => {
      const input = "invalid-date";
      expect(() => vmDate(input).nano().toISOString()).toThrow("Invalid time format");
    });

    it("should handle one-digit fraction", () => {
      const input = "2024-09-19T14:41:13.7Z";
      const expected = "2024-09-19T14:41:13.700000000Z";
      expect(vmDate(input).nano().toISOString()).toBe(expected);
    });

    it("should handle 10-digit fraction by trimming", () => {
      const input = "2024-09-19T14:41:13.1234567891Z";
      const expected = "2024-09-19T14:41:13.123456789Z"; // extra digits trimmed
      expect(vmDate(input).nano().toISOString()).toBe(expected);
    });
  });

});

describe("formatDateWithNanoseconds", () => {
  beforeAll(() => {
    // Make timezone deterministic for tests
    // UTC ensures input Z timestamps stay the same wall time
    vmDate.tz.setDefault("UTC");
  });

  afterAll(() => {
    vmDate.tz.setDefault();
  });

  it("appends 9-digit fraction when format includes .SSS and input has 6 digits", () => {
    const input = "2025-09-15T10:00:00.123456Z";
    const fmt = "YYYY-MM-DD HH:mm:ss.SSS";
    const res = vmDate(input).nano().format(fmt);
    expect(res).toBe("2025-09-15 10:00:00.123456000");
  });

  it("does not append fraction when format does not include .SSS", () => {
    const input = "2025-09-15T10:00:00.123456Z";
    const fmt = "YYYY-MM-DD HH:mm:ss";
    const res = vmDate(input).nano().format(fmt);
    expect(res).toBe("2025-09-15 10:00:00");
  });

  it("pads to 9 digits when input has no fractional seconds", () => {
    const input = "2025-09-15T10:00:00Z";
    const fmt = "YYYY-MM-DD HH:mm:ss.SSS";
    const res = vmDate(input).nano().format(fmt);
    expect(res).toBe("2025-09-15 10:00:00.000000000");
  });

  it("pads to 9 digits when input has fewer than 3 digits", () => {
    const input = "2025-09-15T10:00:00.12Z"; // 2 digits
    const fmt = "YYYY-MM-DD HH:mm:ss.SSS";
    const res = vmDate(input).nano().format(fmt);
    expect(res).toBe("2025-09-15 10:00:00.120000000");
  });

  it("works with formats containing literal text and .SSS", () => {
    const input = "2025-09-15T10:00:00.9Z";
    const fmt = "[Logged at] YYYY/MM/DD HH:mm:ss.SSS";
    const res = vmDate(input).nano().format(fmt);
    expect(res).toBe("Logged at 2025/09/15 10:00:00.900000000");
  });

  it("handles offset timestamps by preserving the original fraction and converting time to default TZ", () => {
    // +02:00 means local time 10:00 corresponds to 08:00 UTC
    const input = "2025-09-15T10:00:00.123456+02:00";
    const fmt = "YYYY-MM-DD HH:mm:ss.SSS";
    const res = vmDate(input).nano().format(fmt);
    expect(res).toBe("2025-09-15 08:00:00.123456000");
  });
});
