import { TextFragment, TextSelection, TextSelectionRange } from "./types";

export const isStartBeforeEnd = (startSelection: TextSelection, endSelection: TextSelection): boolean => {
  if (startSelection.elementIndex < endSelection.elementIndex) {
    return true;
  }
  if (startSelection.elementIndex > endSelection.elementIndex) {
    return false;
  }
  return startSelection.positionIndex <= endSelection.positionIndex;
};

export const getSelectionPosition = (startPosition: TextSelection, endPosition: TextSelection) => {
  if (isStartBeforeEnd(startPosition, endPosition)) {
    return {
      start: startPosition,
      end: endPosition,
    };
  }

  return {
    start: endPosition,
    end: startPosition,
  };
};

export const getSelectionForShiftKey = ({ start, end }: {
  start: TextSelection | null,
  end: TextSelection | null
}, position: TextSelection) => {
  if (!start) {
    return {
      start: position,
      end: null,
    };
  }

  if (!end) {
    return {
      start,
      end: position
    };
  }

  const {
    start: currentStart,
    end: currentEnd
  } = getSelectionPosition(start, end);

  if (position.elementIndex < currentStart.elementIndex ||
    (position.elementIndex === currentStart.elementIndex && position.positionIndex < currentStart.positionIndex)) {
    return {
      start: position,
      end: currentEnd,
    };
  }

  if (position.elementIndex > currentEnd.elementIndex ||
    (position.elementIndex === currentEnd.elementIndex && position.positionIndex > currentEnd.positionIndex)) {
    return {
      start: currentStart,
      end: position
    };
  }

  const distanceToStart = Math.abs(position.elementIndex - currentStart.elementIndex);
  const distanceToEnd = Math.abs(position.elementIndex - currentEnd.elementIndex);

  if (distanceToStart <= distanceToEnd) {
    return {
      start: position,
      end: currentEnd
    };
  } else {
    return {
      start: currentStart,
      end: position
    };
  }
};

export const getSelectionData = (data: string[], startSelection: TextSelection, endSelection: TextSelection): string => {
  const { start, end } = getSelectionPosition(startSelection, endSelection);
  if (start.elementIndex === end.elementIndex) {
    return data[start.elementIndex].slice(start.positionIndex, end.positionIndex);
  }

  return data[start.elementIndex].slice(start.positionIndex) + "\n"
    + data.slice(start.elementIndex, end.elementIndex).join("\n")
    + data[end.elementIndex].slice(0, end.positionIndex);
};

const collectAndSortPositions = (text: string, searchRanges: TextSelectionRange[], selectionRange?: TextSelectionRange) => {
  const positions = new Set<number>();

  searchRanges.forEach(range => {
    positions.add(range.start);
    positions.add(range.end);
  });

  if (selectionRange) {
    positions.add(selectionRange.start);
    positions.add(selectionRange.end);
  }

  positions.add(0);
  positions.add(text.length);

  return Array.from(positions).sort((a, b) => a - b);
};

const isRangeInSelection = (start: number, end: number, selectionRange?: TextSelectionRange): boolean => {
  return selectionRange ? start >= selectionRange.start && end <= selectionRange.end : false;
};

const isRangeInSearch = (start: number, end: number, searchRanges: TextSelectionRange[]): boolean => {
  return searchRanges.some(range => start >= range.start && end <= range.end);
};

const determineHighlightType = (isInSelection: boolean, isInSearch: boolean): "search" | "selection" | "both" | null => {
  if (isInSelection && isInSearch) return "both";
  if (isInSelection) return "selection";
  if (isInSearch) return "search";
  return null;
};

export const getOverlappedFragments = (text: string, searchRanges: TextSelectionRange[], selectionRange?: TextSelectionRange) => {
  if (text.length === 0) {
    return [{
      text: "",
      highlight: null,
      start: 0,
      end: 0
    }];
  }

  const fragments: TextFragment[] = [];
  const sortedPositions = collectAndSortPositions(text, searchRanges, selectionRange);

  for (let i = 0; i < sortedPositions.length - 1; i++) {
    const start = sortedPositions[i];
    const end = sortedPositions[i + 1];
    if (start === end) continue;

    const isInSelection = isRangeInSelection(start, end, selectionRange);
    const isInSearch = isRangeInSearch(start, end, searchRanges);
    const highlight = determineHighlightType(isInSelection, isInSearch);

    fragments.push({
      text: text.slice(start, end),
      highlight,
      start: start,
      end: end
    });
  }

  return fragments;
};

/**
 * Determines the current or next focus entry in a searchable dataset based on the user's search query and navigation direction.
 * This function is commonly used for navigating search results within a list or document.
 *
 * @param {string[]} data - The array of strings to be searched.
 * @param {string} searchValue - The search query to locate within the data array.
 * @param {TextSelection | null} prevFocusPosition - The previous focus position or `null` if starting a new search.
 * @param {boolean} [forward=true] - A boolean flag indicating the navigation direction.
 *                                    If `true`, search proceeds forward; if `false`, it proceeds backward.
 * @returns {TextSelection | null} - Returns the new focus position if found, or `null` if no match is identified.
 */
export const getCurrentFocusEntry = (
  data: string[],
  searchValue: string,
  prevFocusPosition: TextSelection | null,
  forward: boolean = true
): TextSelection | null => {
  if (!searchValue || searchValue.length === 0) {
    return null;
  }

  const normalizedSearchValue = searchValue.toLowerCase();

  if (!prevFocusPosition) {
    return findFirstOccurrence(data, normalizedSearchValue);
  }

  return forward
    ? findNextOccurrence(data, normalizedSearchValue, prevFocusPosition)
    : findPreviousOccurrence(data, normalizedSearchValue, prevFocusPosition);
};

const findFirstOccurrence = (data: string[], searchValue: string): TextSelection | null => {
  for (let elementIndex = 0; elementIndex < data.length; elementIndex++) {
    const item = data[elementIndex].toLowerCase();
    const foundIndex = item.indexOf(searchValue);
    if (foundIndex !== -1) {
      return { elementIndex, positionIndex: foundIndex };
    }
  }
  return null;
};

const findNextOccurrence = (data: string[], searchValue: string, prevPosition: TextSelection): TextSelection | null => {
  const searchInElementAfterPosition = (elementIndex: number, startPosition: number): TextSelection | null => {
    const item = data[elementIndex].toLowerCase();
    const foundIndex = item.indexOf(searchValue, startPosition);
    return foundIndex !== -1 ? { elementIndex, positionIndex: foundIndex } : null;
  };

  const searchInElementBeforePosition = (elementIndex: number, endPosition: number): TextSelection | null => {
    const item = data[elementIndex].toLowerCase();
    const searchArea = item.substring(0, endPosition);
    const foundIndex = searchArea.indexOf(searchValue);
    return foundIndex !== -1 ? { elementIndex, positionIndex: foundIndex } : null;
  };

  const searchInElementRange = (startIndex: number, endIndex: number): TextSelection | null => {
    for (let elementIndex = startIndex; elementIndex <= endIndex; elementIndex++) {
      const result = searchInElementAfterPosition(elementIndex, 0);
      if (result) return result;
    }
    return null;
  };

  // Phase 1: Search in current element after current position
  const currentResult = searchInElementAfterPosition(prevPosition.elementIndex, prevPosition.positionIndex + 1);
  if (currentResult) return currentResult;

  // Phase 2: Search in subsequent elements
  const subsequentResult = searchInElementRange(prevPosition.elementIndex + 1, data.length - 1);
  if (subsequentResult) return subsequentResult;

  // Phase 3: Wrap-around search from beginning
  for (let elementIndex = 0; elementIndex <= prevPosition.elementIndex; elementIndex++) {
    if (elementIndex === prevPosition.elementIndex) {
      const result = searchInElementBeforePosition(elementIndex, prevPosition.positionIndex + 1);
      if (result) return result;
    } else {
      const result = searchInElementAfterPosition(elementIndex, 0);
      if (result) return result;
    }
  }

  return null;
};

const findPreviousOccurrence = (data: string[], searchValue: string, prevPosition: TextSelection): TextSelection | null => {
  const searchInElementBeforePosition = (elementIndex: number, endPosition: number): TextSelection | null => {
    const item = data[elementIndex].toLowerCase();
    const searchArea = item.substring(0, endPosition);
    const foundIndex = searchArea.lastIndexOf(searchValue);
    return foundIndex !== -1 ? { elementIndex, positionIndex: foundIndex } : null;
  };

  const searchInElementAfterPosition = (elementIndex: number, startPosition: number): TextSelection | null => {
    const item = data[elementIndex].toLowerCase();
    const searchArea = item.substring(startPosition);
    const foundIndex = searchArea.lastIndexOf(searchValue);
    return foundIndex !== -1 ? { elementIndex, positionIndex: startPosition + foundIndex } : null;
  };

  const searchInElementRange = (startIndex: number, endIndex: number): TextSelection | null => {
    for (let elementIndex = startIndex; elementIndex >= endIndex; elementIndex--) {
      const item = data[elementIndex].toLowerCase();
      const foundIndex = item.lastIndexOf(searchValue);
      if (foundIndex !== -1) {
        return { elementIndex, positionIndex: foundIndex };
      }
    }
    return null;
  };

  // Phase 1: Search in current element before current position
  const currentResult = searchInElementBeforePosition(prevPosition.elementIndex, prevPosition.positionIndex);
  if (currentResult) return currentResult;

  // Phase 2: Search in previous elements (in reverse order)
  const previousResult = searchInElementRange(prevPosition.elementIndex - 1, 0);
  if (previousResult) return previousResult;

  // Phase 3: Wrap-around search from the end
  for (let elementIndex = data.length - 1; elementIndex >= prevPosition.elementIndex; elementIndex--) {
    if (elementIndex === prevPosition.elementIndex) {
      const result = searchInElementAfterPosition(elementIndex, prevPosition.positionIndex);
      if (result) return result;
    } else {
      const item = data[elementIndex].toLowerCase();
      const foundIndex = item.lastIndexOf(searchValue);
      if (foundIndex !== -1) {
        return { elementIndex, positionIndex: foundIndex };
      }
    }
  }

  return null;
};
