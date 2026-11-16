import { describe, expect, it } from "vitest";
import { getContextData, hasBalancedQuotes, splitLogicalParts } from "./parser";
import { ContextType, LogicalPart, LogicalPartType } from "../types";

function getValue(p: LogicalPart): string {
  return p.value;
}

function getIsPipePart(p: LogicalPart): boolean {
  return p.type === LogicalPartType.Pipe || p.type === LogicalPartType.FilterOrPipe;
}

const values = (parts: readonly LogicalPart[]) => parts.map(getValue);
const pipeFlags = (parts: readonly LogicalPart[]) => parts.map(getIsPipePart);

describe.each(["* |", "* | sort _time |"])("pipe context after %s", prefix => {
  it.each([
    ["extract", ContextType.PipeName],
    ["extract_reg", ContextType.PipeName],
    [" extract_reg", ContextType.PipeName],
    ["extract_regexp", ContextType.PipeName],
    ["extract ", ContextType.PipeValue],
    [" extract ", ContextType.PipeValue],
    [" EXTRACT ", ContextType.PipeValue],
    [" Extract ", ContextType.PipeValue],
    [" EXTRACT_REG", ContextType.PipeName],
    [" EXTRACT_REGEXP ", ContextType.PipeValue],
    ["extract_regexp ", ContextType.PipeValue],
    [" extract_regexp ", ContextType.PipeValue],
    ["\t extract_regexp\t", ContextType.PipeValue],
    ["\n extract_regexp ", ContextType.PipeValue],
    [" coalesce(", ContextType.PipeValue],
    [" COALESCE(", ContextType.PipeValue],
    [" sort _time", ContextType.PipeValue],
    ["extract_regexp_invalid ", ContextType.PipeName],
  ])("classifies %j as %s", (value, expectedType) => {
    const parts = splitLogicalParts(`${prefix}${value}`);
    const part = parts[parts.length - 1];
    const expectedContext = prefix === "* | sort _time |" && expectedType === ContextType.PipeName
      ? ContextType.FilterOrPipeName
      : expectedType;

    expect(getContextData(part, part.value.length).contextType).toBe(expectedContext);
  });
});

describe("splitLogicalParts", () => {
  it.each(["\n", "\r\n", "\t", "\n  ", "\r\n\t", "\n\n\t", " \t\n  "])(
    "separates filters on %j while preserving whitespace and positions",
    whitespace => {
      const query = `os.type:"lin"${whitespace}_time:5m`;
      const parts = splitLogicalParts(query);
      const filters = parts.filter(part => part.type === LogicalPartType.Filter);

      expect(filters.map(part => part.value.trimEnd())).toEqual(["os.type:\"lin\"", "_time:5m"]);
      expect(filters[1].position[0]).toBe(query.indexOf("_time"));
      expect(parts.map(part => `${part.separator ?? ""}${part.value}`).join("")).toBe(query);
    }
  );

  it.each(["\"first\nsecond\"", "'first\nsecond'", "`first\nsecond`", "(first\nsecond)", "{app=\"first\",\n host=\"second\"}"])(
    "preserves whitespace inside quotes and brackets in %j",
    value => {
      expect(values(splitLogicalParts(`${value}\n_time:5m`))).toEqual([value, "\n", "_time:5m"]);
    }
  );

  it("keeps multiline pipe arguments in a single pipe part", () => {
    expect(values(splitLogicalParts("* | stats\n by (os.type)\n count()"))).toEqual([
      "* ",
      " stats\n by (os.type)\n count()",
    ]);
  });

  it.each(["\t", "\n", "\r\n  "])("preserves %j around a colon", whitespace => {
    const query = `os.type${whitespace}:${whitespace}"linux"`;
    expect(values(splitLogicalParts(query))).toEqual([query]);
  });

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
