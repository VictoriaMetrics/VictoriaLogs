import { describe, it, expect } from "vitest";
import { mergeSearchParams } from "./query-string";

describe("mergeSearchParams", () => {
  it("append: keeps all values including duplicates", () => {
    const a = new URLSearchParams("a=1&b=2");
    const b = new URLSearchParams("b=9&b=10&c=3");

    const merged = mergeSearchParams(a, b, "append");

    expect(merged.toString()).toBe("a=1&b=2&b=9&b=10&c=3");
    expect(merged.getAll("b")).toEqual(["2", "9", "10"]);
  });

  it("overwrite: replaces with the last value from `b`", () => {
    const a = new URLSearchParams("a=1&b=2&b=3");
    const b = new URLSearchParams("b=9&b=10&c=4");

    const merged = mergeSearchParams(a, b, "overwrite");

    expect(merged.get("b")).toBe("10");
    expect(merged.getAll("b")).toEqual(["10"]);
    expect(merged.get("a")).toBe("1");
    expect(merged.get("c")).toBe("4");
    expect(merged.toString()).toBe("a=1&b=10&c=4");
  });

  it("does not mutate inputs", () => {
    const a = new URLSearchParams("x=1");
    const b = new URLSearchParams("x=2&y=3");

    const aBefore = a.toString();
    const bBefore = b.toString();
    const merged = mergeSearchParams(a, b, "append");

    expect(a.toString()).toBe(aBefore);
    expect(b.toString()).toBe(bBefore);
    expect(merged.toString()).toBe("x=1&x=2&y=3");
  });

  it("handles empty sources", () => {
    const empty = new URLSearchParams();
    const some = new URLSearchParams("a=1");

    expect(mergeSearchParams(empty, some, "append").toString()).toBe("a=1");
    expect(mergeSearchParams(some, empty, "append").toString()).toBe("a=1");

    expect(mergeSearchParams(empty, some, "overwrite").toString()).toBe("a=1");
    expect(mergeSearchParams(some, empty, "overwrite").toString()).toBe("a=1");
  });

  it("preserves ordering: first from `a`, then from `b`", () => {
    const a = new URLSearchParams("a=1&b=2");
    const b = new URLSearchParams("c=3&b=4");

    const appended = mergeSearchParams(a, b, "append");
    expect(appended.toString()).toBe("a=1&b=2&c=3&b=4");

    const overwritten = mergeSearchParams(a, b, "overwrite");
    // `b` keeps its position from `a`, value comes from `b`
    expect(overwritten.toString()).toBe("a=1&b=4&c=3");
  });

  it("properly handles special characters and spaces", () => {
    const a = new URLSearchParams([["q", "café au lait"]]);
    const b = new URLSearchParams([["q", "☕️"], ["x y", "1 2"]]);

    const appended = mergeSearchParams(a, b, "append");
    expect(appended.getAll("q")).toEqual(["café au lait", "☕️"]);
    expect(appended.has("x y")).toBe(true);

    const overwritten = mergeSearchParams(a, b, "overwrite");
    expect(overwritten.get("q")).toBe("☕️");
    expect(overwritten.get("x y")).toBe("1 2");
  });

  it("works with empty strings and numbers", () => {
    const a = new URLSearchParams([["n", "0"], ["e", ""]]);
    const b = new URLSearchParams([["n", "42"], ["e", ""]]);

    const appended = mergeSearchParams(a, b, "append");
    expect(appended.getAll("n")).toEqual(["0", "42"]);
    expect(appended.getAll("e")).toEqual(["", ""]);

    const overwritten = mergeSearchParams(a, b, "overwrite");
    expect(overwritten.get("n")).toBe("42");
    expect(overwritten.get("e")).toBe("");
  });
});
