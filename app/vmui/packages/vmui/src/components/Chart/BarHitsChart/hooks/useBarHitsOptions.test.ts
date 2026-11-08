import { describe, expect, it } from "vitest";
import { getLabelFromLogHit } from "./useBarHitsOptions";
import { LogHits } from "../../../../api/types";

const hit = (fields: Record<string, string>): LogHits => ({ fields, timestamps: [], values: [], total: 0, _isOther: false });

describe("getLabelFromLogHit", () => {
  it("returns the value for a single field", () => {
    expect(getLabelFromLogHit(hit({ app: "api" }))).toBe("api");
    expect(getLabelFromLogHit(hit({ app: "" }))).toBe("\"\"");
  });

  it("returns field=value pairs for multiple fields", () => {
    expect(getLabelFromLogHit(hit({ app: "api", level: "" }))).toBe("app=\"api\", level=\"\"");
  });

  it("keeps the _stream value as is among multiple fields", () => {
    expect(getLabelFromLogHit(hit({ _stream: "{app=\"api\"}", level: "info" }))).toBe("{app=\"api\"}, level=\"info\"");
  });

  it("returns distinct labels when values contain the separator", () => {
    const a = getLabelFromLogHit(hit({ x: "a, b", y: "c" }));
    const b = getLabelFromLogHit(hit({ x: "a", y: "b, c" }));
    expect(a).not.toBe(b);
  });
});
