import { TextFragment, TextSelection, TextSelectionRange } from "./types";

export const getSelectionPosition = (startPosition: TextSelection, endPosition: TextSelection) => {
  if (startPosition.elementIndex > endPosition.elementIndex) {
    return {
      start: endPosition,
      end: startPosition,
    };
  }

  if (startPosition.elementIndex < endPosition.elementIndex) {
    return {
      start: startPosition,
      end: endPosition,
    };
  }

  if (startPosition.positionIndex > endPosition.positionIndex) {
    return {
      start: endPosition,
      end: startPosition,
    };
  }

  return {
    start: startPosition,
    end: endPosition,
  };
};


const getDataIdEl = (el: HTMLElement, deps = 3) => {
  const dataId = el.getAttribute("data-id");
  if (dataId) {
    return {
      el,
      dataId: Number(dataId),
    };
  }
  if (deps > 0) {
    const parent = el.parentElement;
    if (parent && parent instanceof HTMLElement) {
      return getDataIdEl(parent, deps - 1);
    }
  }
  return null;
};

export const getMousePosition = (e: MouseEvent): TextSelection | null => {
  const target = e.target;
  const position = document.caretPositionFromPoint(e.clientX, e.clientY);
  if (!(target instanceof HTMLElement) || !position) {
    return null;
  }

  const elementData = getDataIdEl(target);
  if (!elementData) {
    return null;
  }

  const { el, dataId: elementIndex } = elementData;
  const range = document.createRange();
  range.setStart(position.offsetNode, position.offset);

  const tempRange = document.createRange();
  tempRange.selectNodeContents(el);
  tempRange.setEnd(range.startContainer, range.startOffset);

  const clickedIndexStr = tempRange.toString();

  return {
    elementIndex,
    positionIndex: clickedIndexStr.length,
  };
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
 *  Function to parse JSON into the string array
 *  Works almost 2 times faster than JSON.stringify for a simple `key:value` data, because we don't need to screen the data
 *  */
export const parseDataToJsonArray = (data: Record<string, string>[]): string[] => {
  const jsonElementsArr = [];
  jsonElementsArr.push("[");
  data.forEach((el, idx) => {
    jsonElementsArr.push("  {");
    Object.entries(el).forEach(([key, value]) => {
      const valueParts = value.split("\n");
      jsonElementsArr.push(`    ${key}: ${valueParts[0]}`);
      if (valueParts.length > 1) {
        for (let i = 1; i < valueParts.length; i++) {
          jsonElementsArr.push(`    ${" ".repeat(key.length + 2)}${valueParts[i]}`);
        }
      }
    });
    if (idx < data.length - 1) {
      jsonElementsArr.push("  },");
    } else {
      jsonElementsArr.push("  }");
    }
  });
  jsonElementsArr.push("]");
  return jsonElementsArr;
};

export const findAllMatches = (data: string[], searchValue: string): TextSelection[] => {
  const matches: TextSelection[] = [];
  if (!searchValue || searchValue.length === 0) {
    return matches;
  }

  searchValue = searchValue.toLowerCase();
  data.forEach((item, elementIndex) => {
    let startIndex = 0;
    let foundIndex = item.toLowerCase().indexOf(searchValue, startIndex);

    while (foundIndex !== -1) {
      matches.push({
        elementIndex,
        positionIndex: foundIndex
      });

      startIndex = foundIndex + 1;
      foundIndex = item.indexOf(searchValue, startIndex);
    }
  });
  return matches;
};
