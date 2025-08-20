import { TextSelection } from "../types";

export const isClickInSelection = (clickPosition: TextSelection, startSelectionPosition: TextSelection | null, endSelectionPosition: TextSelection | null): boolean => {
  if (!startSelectionPosition || !endSelectionPosition) {
    return false;
  }

  const { elementIndex: clickElement, positionIndex: clickPos } = clickPosition;
  const { elementIndex: startElement, positionIndex: startPos } = startSelectionPosition;
  const { elementIndex: endElement, positionIndex: endPos } = endSelectionPosition;

  // Determine the start and end of selection considering the direction
  const selectionStart = startElement < endElement ||
  (startElement === endElement && startPos <= endPos)
    ? { elementIndex: startElement, positionIndex: startPos }
    : { elementIndex: endElement, positionIndex: endPos };

  const selectionEnd = startElement < endElement ||
  (startElement === endElement && startPos <= endPos)
    ? { elementIndex: endElement, positionIndex: endPos }
    : { elementIndex: startElement, positionIndex: startPos };

  // Check if the click is within the selection bounds
  if (clickElement < selectionStart.elementIndex || clickElement > selectionEnd.elementIndex) {
    return false;
  }

  if (clickElement === selectionStart.elementIndex && clickElement === selectionEnd.elementIndex) {
    // Click is on the same line as both start and end of selection
    return clickPos >= selectionStart.positionIndex && clickPos <= selectionEnd.positionIndex;
  } else if (clickElement === selectionStart.elementIndex) {
    // Click is on the line where selection starts
    return clickPos >= selectionStart.positionIndex;
  } else if (clickElement === selectionEnd.elementIndex) {
    // Click is on the line where selection ends
    return clickPos <= selectionEnd.positionIndex;
  }

  // Click is on an intermediate line
  return true;
};
