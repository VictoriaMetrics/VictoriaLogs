import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { vmDate } from "../../../../utils/time";
import { getTooltipTimeRangeLines } from "./getTooltipTimeRangeLines";

describe("getTooltipTimeRangeLines", () => {
  beforeAll(() => {
    vmDate.tz.setDefault("UTC");
  });

  afterAll(() => {
    vmDate.tz.setDefault();
  });

  it("omits seconds for minute-aligned intervals", () => {
    expect(getTooltipTimeRangeLines(1764547200, 1764554400, 7200)).toBe(
      "2025-12-01 00:00 - 02:00",
    );
  });

  it("keeps seconds for non-minute-aligned intervals", () => {
    expect(getTooltipTimeRangeLines(1764547200, 1764547290, 90)).toBe(
      "2025-12-01 00:00:00 - 00:01:30",
    );
  });

  it("uses millisecond precision for sub-second intervals", () => {
    expect(getTooltipTimeRangeLines(1764547200.1, 1764547200.2, 0.1)).toBe(
      "2025-12-01 00:00:00.100 - 00:00:00.200",
    );
  });

  it("uses nanosecond precision for sub-millisecond intervals", () => {
    expect(getTooltipTimeRangeLines(0.0000001, 0.0000002, 0.0000001)).toBe(
      "1970-01-01 00:00:00.000000100 - 00:00:00.000000200",
    );
  });
});
