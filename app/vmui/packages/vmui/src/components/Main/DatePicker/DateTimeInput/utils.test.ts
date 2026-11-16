import { afterEach, describe, expect, it } from "vitest";
import { vmDate } from "../../../../utils/time";
import { formatDateTimeInputValue, parseDateTimeInputValue } from "./utils";

describe("DateTimeInput utils", () => {
  afterEach(() => {
    vmDate.tz.setDefault();
  });

  it("parses a wall-clock draft in the selected timezone", () => {
    vmDate.tz.setDefault("America/New_York");

    expect(parseDateTimeInputValue("2026-07-22 10:31:01.168000000"))
      .toBe("2026-07-22T14:31:01.168000000Z");
  });

  it("formats an ISO value in the selected timezone", () => {
    vmDate.tz.setDefault("America/New_York");

    expect(formatDateTimeInputValue("2026-07-22T14:31:01.168000000Z"))
      .toBe("2026-07-22 10:31:01.168000000");
  });

  it("rejects empty and invalid drafts", () => {
    expect(parseDateTimeInputValue("")).toBeNull();
    expect(parseDateTimeInputValue("invalid")).toBeNull();
  });
});
