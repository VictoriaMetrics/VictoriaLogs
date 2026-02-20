import { describe, expect, it } from "vitest";
import { arrayMove, isDecreasing } from "./array";

describe("isDecreasing", () => {
  it("should return true for an array with strictly decreasing numbers", () => {
    expect(isDecreasing([5, 4, 3, 2, 1])).toBe(true);
  });

  it("should return false for an array with increasing numbers", () => {
    expect(isDecreasing([1, 2, 3, 4, 5])).toBe(false);
  });

  it("should return false for an array with equal consecutive numbers", () => {
    expect(isDecreasing([5, 5, 4, 3, 2])).toBe(false);
  });

  it("should return false for an empty array", () => {
    expect(isDecreasing([])).toBe(false);
  });

  it("should return false for an array with a single element", () => {
    expect(isDecreasing([1])).toBe(false);
  });

  it("should return false for an array with both increasing and decreasing numbers", () => {
    expect(isDecreasing([5, 3, 4, 2, 1])).toBe(false);
  });

  it("should return true for an array with negative strictly decreasing numbers", () => {
    expect(isDecreasing([-1, -2, -3, -4])).toBe(true);
  });

  it("should return false for an array with a mix of positive and negative numbers that do not strictly decrease", () => {
    expect(isDecreasing([3, 2, -1, -1])).toBe(false);
  });
});

describe("arrayMove", () => {
  it("should move an item forward (to a higher index)", () => {
    expect(arrayMove([1, 2, 3, 4], 1, 3)).toEqual([1, 3, 4, 2]);
  });

  it("should move an item backward (to a lower index)", () => {
    expect(arrayMove([1, 2, 3, 4], 3, 1)).toEqual([1, 4, 2, 3]);
  });

  it("should move first item to last", () => {
    expect(arrayMove([1, 2, 3, 4], 0, 3)).toEqual([2, 3, 4, 1]);
  });

  it("should move last item to first", () => {
    expect(arrayMove([1, 2, 3, 4], 3, 0)).toEqual([4, 1, 2, 3]);
  });

  it("should return a new array (not mutate input)", () => {
    const input = [1, 2, 3];
    const result = arrayMove(input, 0, 2);

    expect(result).toEqual([2, 3, 1]);
    expect(result).not.toBe(input);
    expect(input).toEqual([1, 2, 3]);
  });

  it("should keep array unchanged when from === to", () => {
    expect(arrayMove([1, 2, 3], 1, 1)).toEqual([1, 2, 3]);
  });

  it("should work with generic types", () => {
    expect(arrayMove(["a", "b", "c"], 2, 0)).toEqual(["c", "a", "b"]);
  });

  it("should handle out-of-range indexes like native splice (from too large)", () => {
    // splice(from, 1) with from >= length removes nothing => item is undefined
    // then splice(to, 0, undefined) inserts undefined
    expect(arrayMove([1, 2, 3], 10, 1)).toEqual([1, undefined, 2, 3]);
  });

  it("should handle negative indexes like native splice", () => {
    // from = -1 removes last element (3), to = 0 inserts at start
    expect(arrayMove([1, 2, 3], -1, 0)).toEqual([3, 1, 2]);
  });
});
