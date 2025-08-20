import { TextSelection } from "../types";

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

export const getWordSelectionAtMouse = (e: MouseEvent): { start: TextSelection; end: TextSelection } | null => {
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

  const fullText = el.textContent || "";

  const range = document.createRange();
  range.setStart(position.offsetNode, position.offset);

  const tempRange = document.createRange();
  tempRange.selectNodeContents(el);
  tempRange.setEnd(range.startContainer, range.startOffset);

  const clickPosition = tempRange.toString().length;

  const wordBoundaries = findWordBoundaries(fullText, clickPosition);

  if (!wordBoundaries) {
    return null;
  }

  return {
    start: {
      elementIndex,
      positionIndex: wordBoundaries.start,
    },
    end: {
      elementIndex,
      positionIndex: wordBoundaries.end,
    },
  };
};


// Helper function to check if we're in a numeric sequence with dots
const isInNumericSequence = (text: string, position: number): boolean => {
  let start = position;
  let end = position;

  // Find the boundaries of the current sequence of digits and dots
  while (start > 0 && /[\d.]/.test(text[start - 1])) {
    start--;
  }

  while (end < text.length && /[\d.]/.test(text[end])) {
    end++;
  }

  const sequence = text.slice(start, end);

  // Check if the sequence contains a dot and consists only of digits and dots
  // This ensures we're in a numeric sequence like "17.88" or "18.8.8.0"
  // and not in mixed text like "87word78.ol"
  return sequence.includes(".") && /^[\d.]+$/.test(sequence);
};

const findWordBoundaries = (text: string, position: number): { start: number; end: number } | null => {
  if (position < 0 || position >= text.length) {
    return null;
  }

  const char = text[position];

  // If position is not on a word, number, or dot, return null
  if (!/[\w.]/.test(char)) {
    return null;
  }

  let start = position;
  let end = position;

  // Check if we are in a number with dots (including when clicking on the dot itself)
  const isInNumberWithDots = isInNumericSequence(text, position);

  if (isInNumberWithDots) {
    // For numbers with dots (IP addresses, versions, etc.)
    while (start > 0 && /[\d.]/.test(text[start - 1])) {
      start--;
    }

    while (end < text.length && /[\d.]/.test(text[end])) {
      end++;
    }

    // Remove leading and trailing dots if they exist
    while (start < end && text[start] === ".") {
      start++;
    }
    while (end > start && text[end - 1] === ".") {
      end--;
    }
  } else {
    // For regular words and mixed alphanumeric text (letters, digits, but no dots)
    while (start > 0 && /\w/.test(text[start - 1])) {
      start--;
    }

    while (end < text.length && /\w/.test(text[end])) {
      end++;
    }
  }

  return { start, end };
};
