import { describe, expect, it } from "vitest";
import { buildFieldValuesQuery } from "./topFieldValuesUtils";

describe("buildFieldValuesQuery", () => {
  it("quotes the field name in the top values query", () => {
    expect(buildFieldValuesQuery("Log entry ID", "top", 10))
      .toBe("\"Log entry ID\":* | top 10 by (\"Log entry ID\")");
  });

  it("quotes the field name in the bottom values query", () => {
    expect(buildFieldValuesQuery("Type d'audit", "bottom", 5))
      .toBe("\"Type d'audit\":* | stats by (\"Type d'audit\") count() as hits | sort by (hits asc, \"Type d'audit\") | limit 5");
  });

  it("quotes ordinary field names (still valid LogsQL)", () => {
    expect(buildFieldValuesQuery("username", "top", 10))
      .toBe("\"username\":* | top 10 by (\"username\")");
  });
});
