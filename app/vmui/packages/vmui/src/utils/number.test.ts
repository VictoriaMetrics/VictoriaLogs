import { describe, it, expect } from "vitest";
import { formatNumber, formatNumberShort, formatPercent } from "./number";

describe("utils/number", () => {
  describe("formatNumber", () => {
    it("formats integers with grouping by default (en-US)", () => {
      expect(formatNumber(0)).toBe("0");
      expect(formatNumber(12)).toBe("12");
      expect(formatNumber(999)).toBe("999");
      expect(formatNumber(1_000)).toBe("1,000");
      expect(formatNumber(1_234_567)).toBe("1,234,567");
    });

    it("allows overriding options (e.g. disable grouping)", () => {
      expect(formatNumber(1_234_567, { useGrouping: false })).toBe("1234567");
    });

    it("respects fraction digit options", () => {
      expect(formatNumber(1_234.56, { maximumFractionDigits: 0 })).toBe("1,235");
      expect(formatNumber(1_234.56, { maximumFractionDigits: 1 })).toBe("1,234.6");
      expect(formatNumber(1_234.56, { minimumFractionDigits: 2 })).toBe("1,234.56");
    });
  });

  describe("formatNumberShort", () => {
    it("returns value as string for numbers < 1000", () => {
      expect(formatNumberShort(0)).toBe("0");
      expect(formatNumberShort(1)).toBe("1");
      expect(formatNumberShort(12)).toBe("12");
      expect(formatNumberShort(999)).toBe("999");
    });

    it("formats thousands with K", () => {
      expect(formatNumberShort(1_000)).toBe("1K");
      expect(formatNumberShort(1_200)).toBe("1.2K");
      expect(formatNumberShort(1_500)).toBe("1.5K");
    });

    it("formats millions with M", () => {
      expect(formatNumberShort(1_000_000)).toBe("1M");
      expect(formatNumberShort(1_200_000)).toBe("1.2M");
      expect(formatNumberShort(1_500_000)).toBe("1.5M");
    });

    it("formats billions with B", () => {
      expect(formatNumberShort(1_000_000_000)).toBe("1B");
      expect(formatNumberShort(1_200_000_000)).toBe("1.2B");
      expect(formatNumberShort(1_500_000_000)).toBe("1.5B");
    });

    it("does not show trailing .0 for whole compact values", () => {
      expect(formatNumberShort(2_000)).toBe("2K");
      expect(formatNumberShort(3_000_000)).toBe("3M");
      expect(formatNumberShort(4_000_000_000)).toBe("4B");
    });

    it("rounds to at most one fraction digit in compact notation", () => {
      // With maximumFractionDigits: 1 these should be stable.
      expect(formatNumberShort(1_010)).toBe("1K");
      expect(formatNumberShort(1_050)).toBe("1.1K");
      expect(formatNumberShort(1_040_000)).toBe("1M");
      expect(formatNumberShort(1_060_000)).toBe("1.1M");
    });
  });

  describe("formatPercent", () => {
    it("returns dash for null", () => {
      expect(formatPercent(null)).toBe("-");
    });

    it("formats >= 1 with default 1 fraction digit", () => {
      expect(formatPercent(1)).toBe("1.0%");
      expect(formatPercent(12.34)).toBe("12.3%");
    });

    it("formats >= 0.01 and < 1 with default 2 fraction digits", () => {
      expect(formatPercent(0.99)).toBe("0.99%");
      expect(formatPercent(0.1)).toBe("0.10%");
      expect(formatPercent(0.01)).toBe("0.01%");
    });

    it("shows threshold label for very small values", () => {
      expect(formatPercent(0.009)).toBe("<0.01%");
      expect(formatPercent(0)).toBe("0%");
    });

    it("handles negative values symmetrically", () => {
      expect(formatPercent(-1)).toBe("-1.0%");
      expect(formatPercent(-0.1)).toBe("-0.10%");
    });

    it("allows overriding fraction digits", () => {
      expect(formatPercent(12.34, 2)).toBe("12.34%");
      expect(formatPercent(0.1234, 3)).toBe("0.123%");
    });
  });
});
