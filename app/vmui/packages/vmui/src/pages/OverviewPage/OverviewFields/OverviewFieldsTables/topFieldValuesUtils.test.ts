import { describe, expect, it } from "vitest";
import { buildFieldValuesQuery } from "./topFieldValuesUtils";

describe("buildFieldValuesQuery", () => {
  it("quotes the field in every clause", () => {
    expect(buildFieldValuesQuery("Log entry ID", "top", 10))
      .toBe("\"Log entry ID\":* | top 10 by (\"Log entry ID\")");
  });

  it("quotes the field in the bottom mode, including the sort", () => {
    expect(buildFieldValuesQuery("Log entry ID", "bottom", 5))
      .toBe("\"Log entry ID\":* | stats by (\"Log entry ID\") count() as hits"
        + " | sort by (hits asc, \"Log entry ID\") | limit 5");
  });

  it("quotes a plain name too", () => {
    expect(buildFieldValuesQuery("level", "top", 3))
      .toBe("\"level\":* | top 3 by (\"level\")");
  });
});
