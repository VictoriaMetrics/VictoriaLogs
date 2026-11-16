import { describe, expect, it } from "vitest";
import { ALLOWED_INTERVALS } from "../constants/intervals";
import { LOGS_INTERVALS_COUNT } from "../constants/logs";
import { getDefaultIntervalOption, getIntervalOptions } from "./intervals";
import { getNanosecondsFromDuration } from "./time";

describe("intervals", () => {
  describe("ALLOWED_INTERVALS", () => {
    it("contains unique ascending positive intervals", () => {
      expect(ALLOWED_INTERVALS).not.toHaveLength(0);
      expect(new Set(ALLOWED_INTERVALS.map(({ duration }) => duration)).size).toBe(ALLOWED_INTERVALS.length);

      for (const { valueNs } of ALLOWED_INTERVALS) {
        expect(valueNs).toBeGreaterThan(0n);
      }

      for (let i = 1; i < ALLOWED_INTERVALS.length; i++) {
        expect(ALLOWED_INTERVALS[i].valueNs).toBeGreaterThan(ALLOWED_INTERVALS[i - 1].valueNs);
      }
    });

    it("does not include non-calendar intervals", () => {
      const durations = ALLOWED_INTERVALS.map(({ duration }) => duration);

      expect(durations).not.toContain("15h");
      expect(durations).not.toContain("1d6h");
      expect(durations).not.toContain("3d");
    });

    it("keeps intervals above 7 days aligned to whole weeks", () => {
      const weekNs = getNanosecondsFromDuration("7d");

      for (const { duration, valueNs } of ALLOWED_INTERVALS) {
        if (valueNs > weekNs) {
          expect(valueNs % weekNs, duration).toBe(0n);
        }
      }
    });
  });

  describe("getIntervalOptions", () => {
    it("returns empty list for invalid ranges", () => {
      expect(getIntervalOptions({ start: 0n, end: 0n })).toEqual([]);
      expect(getIntervalOptions({ start: 10n, end: 1n })).toEqual([]);
    });

    it("returns expected amount of unique ascending intervals", () => {
      const ranges = ["5m", "15m", "30m", "1h", "3h", "6h", "12h", "24h", "2d", "7d", "30d", "90d", "180d", "1y"];

      for (const range of ranges) {
        const intervals = getIntervalOptions({
          start: 0n,
          end: getNanosecondsFromDuration(range),
        });

        expect(intervals).toHaveLength(LOGS_INTERVALS_COUNT);
        expect(new Set(intervals.map(({ duration }) => duration)).size).toBe(intervals.length);

        for (let i = 1; i < intervals.length; i++) {
          expect(intervals[i].valueNs).toBeGreaterThan(intervals[i - 1].valueNs);
        }
      }
    });

    it("includes default interval in options", () => {
      const ranges = ["5m", "15m", "30m", "1h", "3h", "6h", "12h", "24h", "2d", "7d", "30d", "90d", "180d", "1y"];

      for (const range of ranges) {
        const intervals = getIntervalOptions({
          start: 0n,
          end: getNanosecondsFromDuration(range),
        });
        const defaultInterval = getDefaultIntervalOption({
          start: 0n,
          end: getNanosecondsFromDuration(range),
        });

        expect(defaultInterval).toBeDefined();
        expect(intervals.map(({ duration }) => duration)).toContain(defaultInterval?.duration);
      }
    });

    it("keeps default interval in the center for standard ranges", () => {
      const ranges = ["5m", "15m", "30m", "1h", "3h", "6h", "12h", "24h", "2d", "7d", "30d", "90d", "180d", "1y"];

      for (const range of ranges) {
        const intervals = getIntervalOptions({
          start: 0n,
          end: getNanosecondsFromDuration(range),
        });
        const defaultInterval = getDefaultIntervalOption({
          start: 0n,
          end: getNanosecondsFromDuration(range),
        });
        const centerInterval = intervals[Math.floor(intervals.length / 2)];

        expect(defaultInterval).toBeDefined();
        expect(centerInterval?.duration).toBe(defaultInterval?.duration);
      }
    });

    it("does not suggest non-calendar intervals", () => {
      const ranges = ["7d", "30d", "90d", "180d", "1y"];

      for (const range of ranges) {
        const durations = getIntervalOptions({
          start: 0n,
          end: getNanosecondsFromDuration(range),
        }).map(({ duration }) => duration);

        expect(durations).not.toContain("15h");
        expect(durations).not.toContain("1d6h");
        expect(durations).not.toContain("3d");
      }
    });
  });

  describe("getDefaultIntervalOption", () => {
    it("returns undefined for invalid ranges", () => {
      expect(getDefaultIntervalOption({ start: 0n, end: 0n })).toBeUndefined();
      expect(getDefaultIntervalOption({ start: 10n, end: 1n })).toBeUndefined();
    });

    it("returns expected default intervals", () => {
      expect(getDefaultIntervalOption({
        start: 0n,
        end: getNanosecondsFromDuration("5m"),
      })?.duration).toBe("5s");

      expect(getDefaultIntervalOption({
        start: 0n,
        end: getNanosecondsFromDuration("15m"),
      })?.duration).toBe("10s");

      expect(getDefaultIntervalOption({
        start: 0n,
        end: getNanosecondsFromDuration("30m"),
      })?.duration).toBe("15s");

      expect(getDefaultIntervalOption({
        start: 0n,
        end: getNanosecondsFromDuration("1h"),
      })?.duration).toBe("30s");

      expect(getDefaultIntervalOption({
        start: 0n,
        end: getNanosecondsFromDuration("3h"),
      })?.duration).toBe("1m");

      expect(getDefaultIntervalOption({
        start: 0n,
        end: getNanosecondsFromDuration("6h"),
      })?.duration).toBe("5m");

      expect(getDefaultIntervalOption({
        start: 0n,
        end: getNanosecondsFromDuration("12h"),
      })?.duration).toBe("10m");

      expect(getDefaultIntervalOption({
        start: 0n,
        end: getNanosecondsFromDuration("24h"),
      })?.duration).toBe("15m");

      expect(getDefaultIntervalOption({
        start: 0n,
        end: getNanosecondsFromDuration("2d"),
      })?.duration).toBe("30m");

      expect(getDefaultIntervalOption({
        start: 0n,
        end: getNanosecondsFromDuration("7d"),
      })?.duration).toBe("3h");

      expect(getDefaultIntervalOption({
        start: 0n,
        end: getNanosecondsFromDuration("30d"),
      })?.duration).toBe("6h");

      expect(getDefaultIntervalOption({
        start: 0n,
        end: getNanosecondsFromDuration("90d"),
      })?.duration).toBe("1d");

      expect(getDefaultIntervalOption({
        start: 0n,
        end: getNanosecondsFromDuration("180d"),
      })?.duration).toBe("2d");

      expect(getDefaultIntervalOption({
        start: 0n,
        end: getNanosecondsFromDuration("1y"),
      })?.duration).toBe("7d");
    });

    it("keeps default intervals monotonically increasing", () => {
      const ranges = ["5m", "15m", "30m", "1h", "3h", "6h", "12h", "24h", "2d", "7d", "30d", "90d", "180d", "1y"];
      let previousDefaultNs = 0n;

      for (const range of ranges) {
        const defaultInterval = getDefaultIntervalOption({
          start: 0n,
          end: getNanosecondsFromDuration(range),
        });

        expect(defaultInterval).toBeDefined();
        expect(defaultInterval!.valueNs).toBeGreaterThanOrEqual(previousDefaultNs);

        previousDefaultNs = defaultInterval!.valueNs;
      }
    });
  });
});
