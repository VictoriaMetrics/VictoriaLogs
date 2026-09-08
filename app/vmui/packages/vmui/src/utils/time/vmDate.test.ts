import { describe, expect, it } from "vitest";
import { formatDateWithNanoseconds, getNanoTimestamp, vmDate } from "../time";

describe("vmDate", () => {
  it("returns a dayjs-compatible date instance", () => {
    const input = "2024-03-09T16:00:00.250Z";
    const date = vmDate(input);

    expect(date.isValid()).toBe(true);
    expect(date.valueOf()).toBe(1710000000250);
    expect(date.utc().format("YYYY-MM-DD HH:mm:ss.SSS")).toBe("2024-03-09 16:00:00.250");
    expect(vmDate.isDayjs(date)).toBe(true);
  });

  it("keeps the original input in raw()", () => {
    const input = "2024-03-09T16:00:00.250Z";

    expect(vmDate(input).raw()).toBe(input);
    expect(vmDate().raw()).toBeUndefined();
  });

  it("adds raw() and nano() as non-enumerable methods", () => {
    const date = vmDate("2024-03-09T16:00:00.250Z");

    expect(date.raw()).toBe("2024-03-09T16:00:00.250Z");
    expect(typeof date.nano().format).toBe("function");
    expect(typeof date.nano().timestamp).toBe("function");
    expect(typeof date.nano().toISOString).toBe("function");
    expect(Object.keys(date)).not.toContain("raw");
    expect(Object.keys(date)).not.toContain("nano");
    expect(Object.getOwnPropertyDescriptor(date, "raw")?.enumerable).toBe(false);
    expect(Object.getOwnPropertyDescriptor(date, "nano")?.enumerable).toBe(false);
  });

  it("delegates nano operations to nano utilities using the raw input", () => {
    vmDate.tz.setDefault("UTC");

    try {
      const input = "2025-09-15T10:00:00.123456Z";
      const format = "YYYY-MM-DD HH:mm:ss.SSS";
      const nano = vmDate(input).nano();

      expect(nano.format(format)).toBe(formatDateWithNanoseconds(input, format, (baseFormat: string): string => vmDate.tz(input).format(baseFormat)));
      expect(nano.timestamp()).toBe(getNanoTimestamp(input, vmDate(input).valueOf()));
      expect(nano.toISOString()).toBe(vmDate(input).nano().toISOString());
    } finally {
      vmDate.tz.setDefault();
    }
  });

  it("keeps vmDate.tz(value) semantics separate from vmDate(value).tz()", () => {
    vmDate.tz.setDefault("Europe/Warsaw");

    try {
      const input = "2026-05-29T08:00:00Z";

      expect(vmDate.tz(input).format()).toBe("2026-05-29T08:00:00+02:00");
      expect(vmDate(input).tz().format()).toBe("2026-05-29T10:00:00+02:00");
      expect(vmDate.tz(input).raw()).toBe(input);
    } finally {
      vmDate.tz.setDefault();
    }
  });

  it("preserves raw nanoseconds for timezone wall-clock input", () => {
    vmDate.tz.setDefault("Europe/Warsaw");

    try {
      const input = "2026-05-29 10:00:00.123456789";
      const date = vmDate.tz(input);

      expect(date.nano().format("YYYY-MM-DD HH:mm:ss.SSS")).toBe("2026-05-29 10:00:00.123456789");
      expect(date.nano().toISOString()).toBe("2026-05-29T08:00:00.123456789Z");
    } finally {
      vmDate.tz.setDefault();
    }
  });

  it("keeps dayjs static duration API available", () => {
    expect(vmDate.duration({ minutes: 1, seconds: 30 }).asSeconds()).toBe(90);
  });
});
