import { describe, expect, it } from "vitest";
import { isClickInSelection } from "./useContextMenuUtils";
import { TextSelection } from "../types";

describe("isClickInSelection", () => {
  it("should return false if start or end selection is null", () => {
    const clickPosition: TextSelection = { elementIndex: 0, positionIndex: 5 };

    expect(isClickInSelection(clickPosition, null, null)).toBe(false);
    expect(isClickInSelection(clickPosition, { elementIndex: 0, positionIndex: 5 }, null)).toBe(false);
    expect(isClickInSelection(clickPosition, null, { elementIndex: 0, positionIndex: 10 })).toBe(false);
  });

  it("should return true if click is within the range of the selection", () => {
    const clickPosition: TextSelection = { elementIndex: 1, positionIndex: 5 };
    const startSelection: TextSelection = { elementIndex: 0, positionIndex: 0 };
    const endSelection: TextSelection = { elementIndex: 2, positionIndex: 10 };

    expect(isClickInSelection(clickPosition, startSelection, endSelection)).toBe(true);
  });

  it("should return false if click is before the selection", () => {
    const clickPosition: TextSelection = { elementIndex: 0, positionIndex: 0 };
    const startSelection: TextSelection = { elementIndex: 1, positionIndex: 0 };
    const endSelection: TextSelection = { elementIndex: 2, positionIndex: 10 };

    expect(isClickInSelection(clickPosition, startSelection, endSelection)).toBe(false);
  });

  it("should return false if click is after the selection", () => {
    const clickPosition: TextSelection = { elementIndex: 3, positionIndex: 0 };
    const startSelection: TextSelection = { elementIndex: 1, positionIndex: 0 };
    const endSelection: TextSelection = { elementIndex: 2, positionIndex: 10 };

    expect(isClickInSelection(clickPosition, startSelection, endSelection)).toBe(false);
  });

  it("should return true if click is exactly at the start or end of the selection", () => {
    const startSelection: TextSelection = { elementIndex: 1, positionIndex: 5 };
    const endSelection: TextSelection = { elementIndex: 2, positionIndex: 10 };

    expect(isClickInSelection(startSelection, startSelection, endSelection)).toBe(true);
    expect(isClickInSelection(endSelection, startSelection, endSelection)).toBe(true);
  });

  it("should return true if the selection is reversed and click is within the range", () => {
    const clickPosition: TextSelection = { elementIndex: 1, positionIndex: 5 };
    const startSelection: TextSelection = { elementIndex: 2, positionIndex: 10 };
    const endSelection: TextSelection = { elementIndex: 0, positionIndex: 0 };

    expect(isClickInSelection(clickPosition, startSelection, endSelection)).toBe(true);
  });

  it("should return false if click is outside the reversed selection", () => {
    const clickPosition: TextSelection = { elementIndex: 0, positionIndex: 0 };
    const startSelection: TextSelection = { elementIndex: 2, positionIndex: 10 };
    const endSelection: TextSelection = { elementIndex: 1, positionIndex: 5 };

    expect(isClickInSelection(clickPosition, startSelection, endSelection)).toBe(false);
  });
});
