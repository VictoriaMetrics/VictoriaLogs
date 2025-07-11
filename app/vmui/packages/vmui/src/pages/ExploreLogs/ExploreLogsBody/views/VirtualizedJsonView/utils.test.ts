import { describe, expect, it } from "vitest";
import {
  getCurrentFocusEntry,
  getOverlappedFragments,
  getSelectionForShiftKey,
  getSelectionPosition,
} from "./utils";
import { TextSelection, TextSelectionRange } from "./types";

describe("utils", () => {
  describe("getSelectionForShiftKey", () => {
    it("should return start set to position if start is null", () => {
      const position: TextSelection = { elementIndex: 2, positionIndex: 5 };
      const result = getSelectionForShiftKey({ start: null, end: null }, position);

      expect(result).toEqual({
        start: position,
        end: null,
      });
    });

    it("should set end to position if end is null", () => {
      const start: TextSelection = { elementIndex: 1, positionIndex: 2 };
      const position: TextSelection = { elementIndex: 2, positionIndex: 5 };
      const result = getSelectionForShiftKey({ start, end: null }, position);

      expect(result).toEqual({
        start,
        end: position,
      });
    });

    it("should update start to position if position is before current start", () => {
      const start: TextSelection = { elementIndex: 2, positionIndex: 5 };
      const end: TextSelection = { elementIndex: 4, positionIndex: 8 };
      const position: TextSelection = { elementIndex: 1, positionIndex: 3 };
      const result = getSelectionForShiftKey({ start, end }, position);

      expect(result).toEqual({
        start: position,
        end,
      });
    });

    it("should update end to position if position is after current end", () => {
      const start: TextSelection = { elementIndex: 1, positionIndex: 2 };
      const end: TextSelection = { elementIndex: 3, positionIndex: 5 };
      const position: TextSelection = { elementIndex: 4, positionIndex: 6 };
      const result = getSelectionForShiftKey({ start, end }, position);

      expect(result).toEqual({
        start,
        end: position,
      });
    });

    it("should update start if position is closer to start than end", () => {
      const start: TextSelection = { elementIndex: 1, positionIndex: 0 };
      const end: TextSelection = { elementIndex: 5, positionIndex: 0 };
      const position: TextSelection = { elementIndex: 2, positionIndex: 0 };
      const result = getSelectionForShiftKey({ start, end }, position);

      expect(result).toEqual({
        start: position,
        end,
      });
    });

    it("should update end if position is closer to end than start", () => {
      const start: TextSelection = { elementIndex: 1, positionIndex: 0 };
      const end: TextSelection = { elementIndex: 5, positionIndex: 0 };
      const position: TextSelection = { elementIndex: 4, positionIndex: 0 };
      const result = getSelectionForShiftKey({ start, end }, position);

      expect(result).toEqual({
        start,
        end: position,
      });
    });
  });

  describe("getSelectionPosition", () => {
    it("should return positions sorted when startPosition.elementIndex > endPosition.elementIndex", () => {
      const startPosition: TextSelection = { elementIndex: 2, positionIndex: 5 };
      const endPosition: TextSelection = { elementIndex: 1, positionIndex: 10 };
      const result = getSelectionPosition(startPosition, endPosition);

      expect(result).toEqual({ start: endPosition, end: startPosition });
    });

    it("should return positions sorted when startPosition.elementIndex < endPosition.elementIndex", () => {
      const startPosition: TextSelection = { elementIndex: 1, positionIndex: 5 };
      const endPosition: TextSelection = { elementIndex: 2, positionIndex: 10 };
      const result = getSelectionPosition(startPosition, endPosition);

      expect(result).toEqual({ start: startPosition, end: endPosition });
    });

    it("should return positions sorted when elementIndex is equal but startPosition.positionIndex > endPosition.positionIndex", () => {
      const startPosition: TextSelection = { elementIndex: 1, positionIndex: 10 };
      const endPosition: TextSelection = { elementIndex: 1, positionIndex: 5 };
      const result = getSelectionPosition(startPosition, endPosition);

      expect(result).toEqual({ start: endPosition, end: startPosition });
    });

    it("should return positions as is when elementIndex and positionIndex are already sorted", () => {
      const startPosition: TextSelection = { elementIndex: 1, positionIndex: 5 };
      const endPosition: TextSelection = { elementIndex: 1, positionIndex: 10 };
      const result = getSelectionPosition(startPosition, endPosition);

      expect(result).toEqual({ start: startPosition, end: endPosition });
    });
  });

  describe("getOverlappedFragments", () => {
    it("should return fragments with correct highlights when a selection range overlaps a search range", () => {
      const text = "This is some sample text";
      const searchRanges: TextSelectionRange[] = [
        { start: 5, end: 9, type: "search" },
      ];
      const selectionRange: TextSelectionRange = { start: 8, end: 15, type: "selection" };

      const result = getOverlappedFragments(text, searchRanges, selectionRange);

      expect(result).toEqual([
        {
          "end": 5,
          "highlight": null,
          "start": 0,
          "text": "This ",
        },
        {
          "end": 8,
          "highlight": "search",
          "start": 5,
          "text": "is ",
        },
        {
          "end": 9,
          "highlight": "both",
          "start": 8,
          "text": "s",
        },
        {
          "end": 15,
          "highlight": "selection",
          "start": 9,
          "text": "ome sa",
        },
        {
          "end": 24,
          "highlight": null,
          "start": 15,
          "text": "mple text",
        },
      ]);
    });

    it("should handle cases where no selection range is provided", () => {
      const text = "This is some sample text";
      const searchRanges: TextSelectionRange[] = [
        { start: 5, end: 9, type: "search" },
      ];

      const result = getOverlappedFragments(text, searchRanges);

      expect(result).toEqual([
        {
          "end": 5,
          "highlight": null,
          "start": 0,
          "text": "This ",
        },
        {
          "end": 9,
          "highlight": "search",
          "start": 5,
          "text": "is s",
        },
        {
          "end": 24,
          "highlight": null,
          "start": 9,
          "text": "ome sample text",
        },
      ]);
    });

    it("should return no fragments for an empty text input", () => {
      const text = "";
      const searchRanges: TextSelectionRange[] = [
        { start: 0, end: 5, type: "search" },
      ];
      const selectionRange: TextSelectionRange = { start: 2, end: 4, type: "selection" };

      const result = getOverlappedFragments(text, searchRanges, selectionRange);

      expect(result).toEqual([{ text: "", highlight: null, start: 0, end: 0 }]);
    });

    it("should return a single fragment without highlights if no ranges are provided", () => {
      const text = "This is some sample text";

      const result = getOverlappedFragments(text, []);

      expect(result).toEqual([
        {
          "end": 24,
          "highlight": null,
          "start": 0,
          "text": "This is some sample text",
        },
      ]);
    });

    it("should correctly split text into fragments without selection range", () => {
      const text = "abcdef";
      const searchRanges: TextSelectionRange[] = [
        { start: 1, end: 3, type: "search" },
        { start: 4, end: 5, type: "search" },
      ];

      const result = getOverlappedFragments(text, searchRanges);

      expect(result).toEqual([
        {
          "end": 1,
          "highlight": null,
          "start": 0,
          "text": "a",
        },
        {
          "end": 3,
          "highlight": "search",
          "start": 1,
          "text": "bc",
        },
        {
          "end": 4,
          "highlight": null,
          "start": 3,
          "text": "d",
        },
        {
          "end": 5,
          "highlight": "search",
          "start": 4,
          "text": "e",
        },
        {
          "end": 6,
          "highlight": null,
          "start": 5,
          "text": "f",
        },
      ]);
    });
  });

  describe("getCurrentFocusEntry", () => {
    it("should return null if search value is an empty string", () => {
      const data = ["example", "test"];
      const result = getCurrentFocusEntry(data, "", null, true);
      expect(result).toBeNull();
    });

    it("should return the first occurrence when no previous focus position is provided", () => {
      const data = ["hello", "world"];
      const searchValue = "wor";
      const result = getCurrentFocusEntry(data, searchValue, null, true);
      expect(result).toEqual({ elementIndex: 1, positionIndex: 0 });
    });

    it("should return null if the search value is not found in the data", () => {
      const data = ["hello", "world"];
      const searchValue = "notfound";
      const result = getCurrentFocusEntry(data, searchValue, null, true);
      expect(result).toBeNull();
    });

    it("should find the next occurrence when moving forward", () => {
      const data = ["hello", "world", "hello again"];
      const searchValue = "hello";
      const prevFocus: TextSelection = { elementIndex: 0, positionIndex: 0 };
      const result = getCurrentFocusEntry(data, searchValue, prevFocus, true);
      expect(result).toEqual({ elementIndex: 2, positionIndex: 0 });
    });

    it("should find the previous occurrence when moving backward", () => {
      const data = ["hello", "world", "hello again"];
      const searchValue = "hello";
      const prevFocus: TextSelection = { elementIndex: 2, positionIndex: 0 };
      const result = getCurrentFocusEntry(data, searchValue, prevFocus, false);
      expect(result).toEqual({ elementIndex: 0, positionIndex: 0 });
    });

    it("should handle case-insensitive matches correctly", () => {
      const data = ["Hello", "world"];
      const searchValue = "hello";
      const result = getCurrentFocusEntry(data, searchValue, null, true);
      expect(result).toEqual({ elementIndex: 0, positionIndex: 0 });
    });

    it("should return null for an empty data array", () => {
      const data: string[] = [];
      const searchValue = "test";
      const result = getCurrentFocusEntry(data, searchValue, null, true);
      expect(result).toBeNull();
    });

    it("should handle overlapping matches within the same element", () => {
      const data = ["aaabaaa"];
      const searchValue = "aaa";
      const prevFocus: TextSelection = { elementIndex: 0, positionIndex: 0 };
      const result = getCurrentFocusEntry(data, searchValue, prevFocus, true);
      expect(result).toEqual({ elementIndex: 0, positionIndex: 4 });
    });
  });
});
