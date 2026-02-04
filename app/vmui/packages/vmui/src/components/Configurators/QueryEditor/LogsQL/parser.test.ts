import { describe, expect, it } from "vitest";
import { hasBalancedQuotes, splitLogicalParts } from "./parser";
import { LogicalPart, LogicalPartType } from "./types";

function getValue(p: LogicalPart): string {
  return p.value;
}

function getIsPipePart(p: LogicalPart): boolean {
  return p.type === LogicalPartType.Pipe || p.type === LogicalPartType.FilterOrPipe;
}

const values = (parts: readonly LogicalPart[]) => parts.map(getValue);
const pipeFlags = (parts: readonly LogicalPart[]) => parts.map(getIsPipePart);

describe("splitLogicalParts", () => {
  it("splits by spaces into parts", () => {
    const parts = splitLogicalParts("a b c");
    expect(values(parts)).toEqual(["a ", "b ", "c"]);
  });

  it("does not split on spaces adjacent to ':'", () => {
    expect(values(splitLogicalParts("a: b c"))).toEqual(["a: b ", "c"]);
    expect(values(splitLogicalParts("a: b: c d"))).toEqual(["a: b: c ", "d"]);
  });

  it("splits on pipe and marks pipe mode parts", () => {
    const parts = splitLogicalParts("a | b c");
    expect(values(parts)).toEqual(["a ", " b c"]);
    expect(pipeFlags(parts)[0]).toBe(false);
    expect(pipeFlags(parts)[1]).toBe(true);
  });

  it("does not split on spaces inside quotes", () => {
    expect(values(splitLogicalParts("'a b' c"))).toEqual(["'a b' ", "c"]);
    expect(values(splitLogicalParts("`a b` c"))).toEqual(["`a b` ", "c"]);
  });

  it("does not get confused by other quote types inside an open quoted string", () => {
    expect(values(splitLogicalParts("\"a 'b' c\" d"))).toEqual(["\"a 'b' c\" ", "d"]);
    expect(values(splitLogicalParts("'a \"b\" c' d"))).toEqual(["'a \"b\" c' ", "d"]);
  });

  it("does not split on spaces inside brackets (including nested)", () => {
    expect(values(splitLogicalParts("a (b c) d"))).toEqual(["a ", "(b c) ", "d"]);
    expect(values(splitLogicalParts("a (b [c d]) e"))).toEqual(["a ", "(b [c d]) ", "e"]);
  });

  it("does not split on pipe inside quotes/brackets", () => {
    expect(values(splitLogicalParts("a \"b|c d\" e"))).toEqual(["a ", "\"b|c d\" ", "e"]);
    expect(values(splitLogicalParts("a (b|c d) e"))).toEqual(["a ", "(b|c d) ", "e"]);
  });

  it("unbalanced closing bracket should not break splitting", () => {
    expect(values(splitLogicalParts(") a b"))).toEqual([") ", "a ", "b"]);
  });

  it("supports escaped quotes and backslashes inside double-quoted strings", () => {
    const parts = splitLogicalParts("a \"b \\\"c\\\" \\\\ d\" e");
    expect(values(parts)).toEqual(["a ", "\"b \\\"c\\\" \\\\ d\" ", "e"]);
  });

  it("supports escaped quotes and backslashes inside single-quoted strings", () => {
    const parts = splitLogicalParts("a 'b \\'c\\' \\\\ d' e");
    expect(values(parts)).toEqual(["a ", "'b \\'c\\' \\\\ d' ", "e"]);
  });

  it("backtick-quoted strings may contain quotes and backslashes without escaping", () => {
    const parts = splitLogicalParts("a `b \"c\" \\\\ d` e");
    expect(values(parts)).toEqual(["a ", "`b \"c\" \\\\ d` ", "e"]);
  });

  it("does not split inside stream filter {...} even if it contains spaces", () => {
    const parts = splitLogicalParts("{app=\"nginx\", instance=\"host-123:5678\"} _time:5m");
    expect(values(parts)).toEqual([
      "{app=\"nginx\", instance=\"host-123:5678\"} ",
      "_time:5m",
    ]);
  });

  it("does not split inside function calls with arguments and quoted phrases", () => {
    const parts = splitLogicalParts("contains_all(foo, \"bar baz\") _time:1h");
    expect(values(parts)).toEqual(["contains_all(foo, \"bar baz\") ", "_time:1h"]);
  });

  it("does not split inside parentheses used for logical grouping", () => {
    const parts = splitLogicalParts("_time:5m error -(buggy_app OR foobar)");
    expect(values(parts)).toEqual(["_time:5m ", "error ", "-(buggy_app OR foobar)"]);
  });

  it("splits multiple pipes into separate pipe parts", () => {
    const parts = splitLogicalParts("_time:5m error | sort by (_time) desc | limit 10");

    expect(values(parts)).toEqual([
      "_time:5m ",
      "error ",
      " sort by (_time) desc ",
      " limit 10",
    ]);
    expect(pipeFlags(parts)).toEqual([false, false, true, true]);
  });

  it("treats quoted phrases with punctuation/colons as a single part", () => {
    const parts = splitLogicalParts("_time:5m \"error: cannot find file\"");
    expect(values(parts)).toEqual(["_time:5m ", "\"error: cannot find file\""]);
  });

  it("handles exact filter with quoted phrase containing spaces", () => {
    const parts = splitLogicalParts("=\"fatal error: cannot find /foo/bar\" _time:1h");
    expect(values(parts)).toEqual(["=\"fatal error: cannot find /foo/bar\" ", "_time:1h"]);
  });

  it("does not treat # inside quotes as a comment start", () => {
    const parts = splitLogicalParts("\"a # not a comment\" b");
    expect(values(parts)).toEqual(["\"a # not a comment\" ", "b"]);
  });
});

describe("getIsBalanced", () => {
  const getIsBalanced = (str: string) => hasBalancedQuotes(str).isBalancedQuotes;

  it("empty / no quotes", () => {
    expect(getIsBalanced("")).toBe(true);
    expect(getIsBalanced("type:PushEven")).toBe(true);
    expect(getIsBalanced("abc 'def'")).toBe(true);
  });

  it("basic balancing", () => {
    expect(getIsBalanced("type:\"Push'Even")).toBe(false);
    expect(getIsBalanced("type:\"Push'Even\"")).toBe(true);
    expect(getIsBalanced("\"")).toBe(false);
    expect(getIsBalanced("\"\"")).toBe(true);
    expect(getIsBalanced("a\"b")).toBe(false);
    expect(getIsBalanced("a\"b\"")).toBe(true);
  });

  it("escaped quotes inside quoted segment", () => {
    expect(getIsBalanced("type:\"Push\\\"Even\"")).toBe(true);
    expect(getIsBalanced("\"a\\\"b\\\"c\"")).toBe(true);
    expect(getIsBalanced("\"a\\\"b\\\"c")).toBe(false);
  });

  it("multiple segments", () => {
    expect(getIsBalanced("a\"b\"c\"d\"")).toBe(true);
    expect(getIsBalanced("a\"b\"c\"d")).toBe(false);
    expect(getIsBalanced("a\"b\\\"c\"d\"")).toBe(false);
  });

  it("quote preceded by escaped backslash (\\\\) then quote", () => {
    expect(getIsBalanced("a\\\\\"b\"c")).toBe(true);
    expect(getIsBalanced("a\\\\\"b\"c\"")).toBe(false);
  });
});
