import { describe, it, expect } from "vitest";
import dayjs from "dayjs";
import { getNanoTimestamp, toNanoPrecision } from "./time";

describe("Time utils", () => {
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
