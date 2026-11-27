import { describe, expect, it } from "vitest";
import { escapeForLogsQLString, quoteFieldName } from "./regexp";

describe("quoteFieldName", () => {
  it("quotes ordinary field names", () => {
    expect(quoteFieldName("username")).toBe("\"username\"");
  });

  it("quotes field names with spaces", () => {
    expect(quoteFieldName("Log entry ID")).toBe("\"Log entry ID\"");
  });

  it("quotes field names with apostrophes", () => {
    expect(quoteFieldName("Type d'audit")).toBe("\"Type d'audit\"");
  });

  it("quotes field names with colons", () => {
    expect(quoteFieldName("log:level")).toBe("\"log:level\"");
  });

  it("escapes embedded double quotes", () => {
    expect(quoteFieldName("field\"name")).toBe("\"field\\\"name\"");
  });

  it("escapes backslashes", () => {
    expect(quoteFieldName("field\\name")).toBe("\"field\\\\name\"");
  });
});

describe("escapeForLogsQLString", () => {
  it("escapes string values without wrapping them in identifier quotes", () => {
    expect(escapeForLogsQLString("a\"b")).toBe("a\\\"b");
  });
});
