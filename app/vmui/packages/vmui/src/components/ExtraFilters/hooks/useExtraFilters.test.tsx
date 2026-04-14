import { afterEach, describe, expect, it, vi } from "vitest";
import { ExtraFilter, ExtraFilterOperator } from "../types";
import { filterToExpr } from "../utils/buildExprFromExtraFilters";

describe("filterToExpr", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("formats Equals for regular fields with :in()", () => {
    const expr = filterToExpr({
      field: "level",
      operator: ExtraFilterOperator.Equals,
      value: "info",
    } satisfies ExtraFilter);

    expect(expr).toBe("level:in(\"info\")");
  });

  it("formats NotEquals for regular fields as NOT (...:in(...))", () => {
    const expr = filterToExpr({
      field: "level",
      operator: ExtraFilterOperator.NotEquals,
      value: "debug",
    } satisfies ExtraFilter);

    expect(expr).toBe("NOT (level:in(\"debug\"))");
  });

  it("keeps wildcard * as special any-value filter for regular fields", () => {
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
    expect(neq).toBe("NOT (status:*)");
  });

  it("keeps \"\" as special empty-value filter for regular fields", () => {
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

    expect(eq).toBe("msg:\"\"");
    expect(neq).toBe("NOT (msg:\"\")");
  });

  it("escapes inner double quotes for regular Equals", () => {
    const expr = filterToExpr({
      field: "msg",
      operator: ExtraFilterOperator.Equals,
      value: "he said \"hi\"",
    } satisfies ExtraFilter);

    expect(expr).toBe("msg:in(\"he said \\\"hi\\\"\")");
  });

  it("formats Regex for regular fields as grouped OR-regex expression", () => {
    const expr = filterToExpr({
      field: "path",
      operator: ExtraFilterOperator.Regex,
      value: "/api/v[0-9]+",
    } satisfies ExtraFilter);

    expect(expr).toBe("(path:~\"/api/v[0-9]+\")");
  });

  it("formats NotRegex for regular fields as NOT ((...))", () => {
    const expr = filterToExpr({
      field: "path",
      operator: ExtraFilterOperator.NotRegex,
      value: "/admin",
    } satisfies ExtraFilter);

    expect(expr).toBe("NOT ((path:~\"/admin\"))");
  });

  it("warns and falls back to special-value semantics for Regex with *", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    const expr = filterToExpr({
      field: "status",
      operator: ExtraFilterOperator.Regex,
      value: "*",
    } satisfies ExtraFilter);

    expect(expr).toBe("status:*");
    expect(warn).toHaveBeenCalledTimes(1);
  });

  it("warns and falls back to special-value semantics for NotRegex with \"\"", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    const expr = filterToExpr({
      field: "status",
      operator: ExtraFilterOperator.NotRegex,
      value: "\"\"",
    } satisfies ExtraFilter);

    expect(expr).toBe("NOT (status:\"\")");
    expect(warn).toHaveBeenCalledTimes(1);
  });

  it("formats Equals for stream fields with {field in (...)}", () => {
    const expr = filterToExpr({
      field: "type",
      operator: ExtraFilterOperator.Equals,
      value: "PushEvent",
      isStream: true,
    } satisfies ExtraFilter);

    expect(expr).toBe("{type in (\"PushEvent\")}");
  });

  it("formats NotEquals for stream fields with {field not_in (...)}", () => {
    const expr = filterToExpr({
      field: "type",
      operator: ExtraFilterOperator.NotEquals,
      value: "PushEvent",
      isStream: true,
    } satisfies ExtraFilter);

    expect(expr).toBe("{type not_in (\"PushEvent\")}");
  });

  it("formats Regex for stream fields with =~", () => {
    const expr = filterToExpr({
      field: "type",
      operator: ExtraFilterOperator.Regex,
      value: "Push.*",
      isStream: true,
    } satisfies ExtraFilter);

    expect(expr).toBe("{type=~\"Push.*\"}");
  });

  it("formats NotRegex for stream fields with !~", () => {
    const expr = filterToExpr({
      field: "type",
      operator: ExtraFilterOperator.NotRegex,
      value: "Push.*",
      isStream: true,
    } satisfies ExtraFilter);

    expect(expr).toBe("{type!~\"Push.*\"}");
  });

  it("returns _stream selector as-is for Equals", () => {
    const expr = filterToExpr({
      field: "_stream",
      operator: ExtraFilterOperator.Equals,
      value: "{type=\"PushEvent\"}",
    } satisfies ExtraFilter);

    expect(expr).toBe("{type=\"PushEvent\"}");
  });

  it("wraps _stream selector with NOT (...) for NotEquals", () => {
    const expr = filterToExpr({
      field: "_stream",
      operator: ExtraFilterOperator.NotEquals,
      value: "{type=\"PushEvent\"}",
    } satisfies ExtraFilter);

    expect(expr).toBe("NOT ({type=\"PushEvent\"})");
  });

  it("warns and falls back to Equals semantics for Regex on _stream", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    const expr = filterToExpr({
      field: "_stream",
      operator: ExtraFilterOperator.Regex,
      value: "{type=~\"Push.*\"}",
    } satisfies ExtraFilter);

    expect(expr).toBe("{type=~\"Push.*\"}");
    expect(warn).toHaveBeenCalledTimes(1);
  });

  it("warns and falls back to NotEquals semantics for NotRegex on _stream", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    const expr = filterToExpr({
      field: "_stream",
      operator: ExtraFilterOperator.NotRegex,
      value: "{type=~\"Push.*\"}",
    } satisfies ExtraFilter);

    expect(expr).toBe("NOT ({type=~\"Push.*\"})");
    expect(warn).toHaveBeenCalledTimes(1);
  });
});
