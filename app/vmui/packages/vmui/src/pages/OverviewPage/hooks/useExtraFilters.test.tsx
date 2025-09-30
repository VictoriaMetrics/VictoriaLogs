import { describe, it, expect } from "vitest";
import { ExtraFilterOperator, ExtraFilter } from "../FiltersBar/types";
import { filterToExpr } from "./useExtraFilters";

describe("filterToExpr", () => {
  it("formats Equals with := and wraps value in quotes", () => {
    const expr = filterToExpr({
      field: "level",
      operator: ExtraFilterOperator.Equals,
      value: "info",
    } satisfies ExtraFilter);
    expect(expr).toBe("level:=\"info\"");
  });

  it("formats NotEquals with (NOT ...) and uses := when value is not * or \"\"", () => {
    const expr = filterToExpr({
      field: "level",
      operator: ExtraFilterOperator.NotEquals,
      value: "debug",
    } satisfies ExtraFilter);
    expect(expr).toBe("(NOT level:=\"debug\")");
  });

  it("does not use := for wildcard * and preserves raw star", () => {
    const eq = filterToExpr({
      field: "status",
      operator: ExtraFilterOperator.Equals,
      value: "*",
    } satisfies ExtraFilter);
    const neq = filterToExpr({
      field: "status",
      operator: ExtraFilterOperator.NotEquals,
      value: "*",
    } satisfies ExtraFilter);
    expect(eq).toBe("status:*");
    expect(neq).toBe("(NOT status:*)");
  });

  it("does not add extra quotes for empty string token \"\"", () => {
    const eq = filterToExpr({
      field: "msg",
      operator: ExtraFilterOperator.Equals,
      value: "\"\"",
    } satisfies ExtraFilter);
    const neq = filterToExpr({
      field: "msg",
      operator: ExtraFilterOperator.NotEquals,
      value: "\"\"",
    } satisfies ExtraFilter);
    expect(eq).toBe("msg:=\"\"");
    expect(neq).toBe("(NOT msg:=\"\")");
  });

  it("does not wrap _stream values in quotes and does not use :=", () => {
    const eq = filterToExpr({
      field: "_stream",
      operator: ExtraFilterOperator.Equals,
      value: "orders",
    } satisfies ExtraFilter);
    const neq = filterToExpr({
      field: "_stream",
      operator: ExtraFilterOperator.NotEquals,
      value: "billing",
    } satisfies ExtraFilter);
    expect(eq).toBe("_stream:orders");
    expect(neq).toBe("(NOT _stream:billing)");
  });

  it("escapes internal double quotes in quoted values", () => {
    const expr = filterToExpr({
      field: "msg",
      operator: ExtraFilterOperator.Equals,
      value: "he said \"hi\"",
    } satisfies ExtraFilter);
    expect(expr).toBe("msg:=\"he said \\\"hi\\\"\"");
  });

  it("formats Regex with :~ and wraps value in quotes", () => {
    const expr = filterToExpr({
      field: "path",
      operator: ExtraFilterOperator.Regex,
      value: "/api/v[0-9]+",
    } satisfies ExtraFilter);
    expect(expr).toBe("path:~\"/api/v[0-9]+\"");
  });

  it("formats NotRegex with (NOT ...:~...)", () => {
    const expr = filterToExpr({
      field: "path",
      operator: ExtraFilterOperator.NotRegex,
      value: "/admin",
    } satisfies ExtraFilter);
    expect(expr).toBe("(NOT path:~\"/admin\")");
  });
});
