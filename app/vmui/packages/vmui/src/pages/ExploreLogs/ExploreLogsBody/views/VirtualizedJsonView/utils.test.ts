import { describe, expect, it } from "vitest";
import { findAllMatches, getOverlappedFragments, getSelectionPosition, parseDataToJsonArray } from "./utils";
import { TextSelection, TextSelectionRange } from "./types";

describe("utils", () => {
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

  describe("parseDataToJsonArray", () => {
    it("should return an empty JSON array when input is an empty array", () => {
      const result = parseDataToJsonArray([]);
      expect(result).toEqual(["[", "]"]);
    });

    it("should return a JSON array with single object correctly parsed", () => {
      const input = [{ key: "value" }];
      const result = parseDataToJsonArray(input);
      expect(result).toEqual([
        "[",
        "  {",
        "    key: value",
        "  }",
        "]",
      ]);
    });

    it("should correctly parse multiple objects into a JSON array", () => {
      const input: Record<string, string>[] = [
        { key1: "value1", key2: "value2" },
        { key3: "value3" },
      ];
      const result = parseDataToJsonArray(input);
      expect(result).toEqual([
        "[",
        "  {",
        "    key1: value1",
        "    key2: value2",
        "  },",
        "  {",
        "    key3: value3",
        "  }",
        "]",
      ]);
    });

    it("should handle string values with newlines correctly by splitting them into multiple lines", () => {
      const input = [
        { key: "line1\nline2\nline3" },
      ];
      const result = parseDataToJsonArray(input);
      expect(result).toEqual([
        "[",
        "  {",
        "    key: line1",
        "         line2",
        "         line3",
        "  }",
        "]",
      ]);
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

  describe("findAllMatches", () => {
    it("should return no matches when the search value is an empty string", () => {
      const data = ["hello", "world"];
      const searchValue = "";
      const result = findAllMatches(data, searchValue);
      expect(result).toEqual([]);
    });

    it("should return no matches when the search value is not found in the data", () => {
      const data = ["hello", "world"];
      const searchValue = "test";
      const result = findAllMatches(data, searchValue);
      expect(result).toEqual([]);
    });

    it("should find a single match in one element", () => {
      const data = ["hello", "world"];
      const searchValue = "wor";
      const result = findAllMatches(data, searchValue);
      expect(result).toEqual([{ elementIndex: 1, positionIndex: 0 }]);
    });

    it("should find multiple matches in a single element", () => {
      const data = ["abababa"];
      const searchValue = "aba";
      const result = findAllMatches(data, searchValue);
      expect(result).toEqual([
        { elementIndex: 0, positionIndex: 0 },
        { elementIndex: 0, positionIndex: 2 },
        { elementIndex: 0, positionIndex: 4 },
      ]);
    });

    it("should find matches across multiple elements", () => {
      const data = ["hello", "world", "hello world"];
      const searchValue = "lo";
      const result = findAllMatches(data, searchValue);
      expect(result).toEqual([
        { elementIndex: 0, positionIndex: 3 },
        { elementIndex: 2, positionIndex: 3 },
      ]);
    });

    it("should handle case-insensitive matches", () => {
      const data = ["Hello", "World"];
      const searchValue = "hello";
      const result = findAllMatches(data, searchValue);
      expect(result).toEqual([{ elementIndex: 0, positionIndex: 0 }]);
    });

    it("should return no matches for an empty data array", () => {
      const data: string[] = [];
      const searchValue = "test";
      const result = findAllMatches(data, searchValue);
      expect(result).toEqual([]);
    });

    it("should handle scenarios where search string partially overlaps itself in elements", () => {
      const data = ["aaa"];
      const searchValue = "aa";
      const result = findAllMatches(data, searchValue);
      expect(result).toEqual([
        { elementIndex: 0, positionIndex: 0 },
        { elementIndex: 0, positionIndex: 1 },
      ]);
    });
  });
});
