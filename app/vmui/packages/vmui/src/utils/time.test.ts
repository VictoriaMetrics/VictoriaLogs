import { describe, expect, it } from "vitest";
import { getDurationFromMilliseconds, getNanoTimestamp, toNanoPrecision, formatDateWithNanoseconds } from "./time";
import dayjs from "dayjs";

describe("Time utils", () => {
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
  describe("getNanoTimestamp", () => {
    it("should return 0n for an empty string", () => {
      expect(getNanoTimestamp("")).toBe(0n);
    });

    it("correctly handles a date without a fractional part", () => {
      const dateStr = "2023-03-20T12:34:56Z";
      const baseMs = dayjs(dateStr).valueOf();
      const expected = BigInt(baseMs) * 1000000n;
      expect(getNanoTimestamp(dateStr)).toBe(expected);
    });

    it("correctly handles a date with a fractional part of 3 digits", () => {
      // In this case, the fractional part "123" is padded to "123000000",
      // and the remaining part after the first 3 digits is "000000"
      const dateStr = "2023-03-20T12:34:56.123Z";
      const baseMs = dayjs(dateStr).valueOf();
      const expected = BigInt(baseMs) * 1000000n; // extraNano = 0
      expect(getNanoTimestamp(dateStr)).toBe(expected);
    });

    it("correctly computes additional nanoseconds for a fractional part with more than 3 digits", () => {
      // For "123456", the fractional part is padded to "123456000",
      // extraNano = parseInt("456000") = 456000
      const dateStr = "2023-03-20T12:34:56.123456Z";
      const baseMs = dayjs(dateStr).valueOf();
      const extraNano = 456000n;
      const expected = BigInt(baseMs) * 1000000n + extraNano;
      expect(getNanoTimestamp(dateStr)).toBe(expected);
    });

    it("correctly handles a date with a fractional part of 9 digits", () => {
      // For "123456789", extraNano = parseInt("456789") = 456789
      const dateStr = "2023-03-20T12:34:56.123456789Z";
      const baseMs = dayjs(dateStr).valueOf();
      const extraNano = 456789n;
      const expected = BigInt(baseMs) * 1000000n + extraNano;
      expect(getNanoTimestamp(dateStr)).toBe(expected);
    });

    it("returns the default value if the regex does not match (e.g., missing \"Z\")", () => {
      const dateStr = "2023-03-20T12:34:56.123";
      const baseMs = dayjs(dateStr).valueOf();
      const expected = BigInt(baseMs) * 1000000n;
      expect(getNanoTimestamp(dateStr)).toBe(expected);
    });
  });

  describe("toNanoPrecision", () => {
    it("should pad fraction to 9 digits (microseconds -> nanoseconds)", () => {
      const input = "2024-09-19T14:41:13.76572Z";
      const expected = "2024-09-19T14:41:13.765720000Z";
      expect(toNanoPrecision(input)).toBe(expected);
    });

    it("should leave already correct 9-digit fraction untouched", () => {
      const input = "2024-09-19T14:41:13.123456789Z";
      const expected = "2024-09-19T14:41:13.123456789Z";
      expect(toNanoPrecision(input)).toBe(expected);
    });

    it("should pad shorter fractions (milliseconds -> nanoseconds)", () => {
      const input = "2024-09-19T14:41:13.123Z";
      const expected = "2024-09-19T14:41:13.123000000Z";
      expect(toNanoPrecision(input)).toBe(expected);
    });

    it("should add .000000000 if no fraction is present", () => {
      const input = "2024-09-19T14:41:13Z";
      const expected = "2024-09-19T14:41:13.000000000Z";
      expect(toNanoPrecision(input)).toBe(expected);
    });

    it("should throw error on invalid format", () => {
      const input = "invalid-date";
      expect(() => toNanoPrecision(input)).toThrow("Invalid time format");
    });

    it("should handle one-digit fraction", () => {
      const input = "2024-09-19T14:41:13.7Z";
      const expected = "2024-09-19T14:41:13.700000000Z";
      expect(toNanoPrecision(input)).toBe(expected);
    });

    it("should handle 10-digit fraction by trimming", () => {
      const input = "2024-09-19T14:41:13.1234567891Z";
      const expected = "2024-09-19T14:41:13.123456789Z"; // extra digits trimmed
      expect(toNanoPrecision(input)).toBe(expected);
    });
  });

});

describe("formatDateWithNanoseconds", () => {
  beforeAll(() => {
    // Make timezone deterministic for tests
    // UTC ensures input Z timestamps stay the same wall time
    dayjs.tz.setDefault("UTC");
  });

  afterAll(() => {
    dayjs.tz.setDefault();
  });

  it("appends 9-digit fraction when format includes .SSS and input has 6 digits", () => {
    const input = "2025-09-15T10:00:00.123456Z";
    const fmt = "YYYY-MM-DD HH:mm:ss.SSS";
    const res = formatDateWithNanoseconds(input, fmt);
    expect(res).toBe("2025-09-15 10:00:00.123456000");
  });

  it("does not append fraction when format does not include .SSS", () => {
    const input = "2025-09-15T10:00:00.123456Z";
    const fmt = "YYYY-MM-DD HH:mm:ss";
    const res = formatDateWithNanoseconds(input, fmt);
    expect(res).toBe("2025-09-15 10:00:00");
  });

  it("pads to 9 digits when input has no fractional seconds", () => {
    const input = "2025-09-15T10:00:00Z";
    const fmt = "YYYY-MM-DD HH:mm:ss.SSS";
    const res = formatDateWithNanoseconds(input, fmt);
    expect(res).toBe("2025-09-15 10:00:00.000000000");
  });

  it("pads to 9 digits when input has fewer than 3 digits", () => {
    const input = "2025-09-15T10:00:00.12Z"; // 2 digits
    const fmt = "YYYY-MM-DD HH:mm:ss.SSS";
    const res = formatDateWithNanoseconds(input, fmt);
    expect(res).toBe("2025-09-15 10:00:00.120000000");
  });

  it("works with formats containing literal text and .SSS", () => {
    const input = "2025-09-15T10:00:00.9Z";
    const fmt = "[Logged at] YYYY/MM/DD HH:mm:ss.SSS";
    const res = formatDateWithNanoseconds(input, fmt);
    expect(res).toBe("Logged at 2025/09/15 10:00:00.900000000");
  });

  it("handles offset timestamps by preserving the original fraction and converting time to default TZ", () => {
    // +02:00 means local time 10:00 corresponds to 08:00 UTC
    const input = "2025-09-15T10:00:00.123456+02:00";
    const fmt = "YYYY-MM-DD HH:mm:ss.SSS";
    const res = formatDateWithNanoseconds(input, fmt);
    expect(res).toBe("2025-09-15 08:00:00.123456000");
  });
});
