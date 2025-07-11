import { useEffect, useState, useRef, RefObject, useCallback } from "preact/compat";
import { TextSelection } from "../types";
import { getMousePosition, getSelectionForShiftKey, getWordSelectionAtMouse } from "../utils";

export const useTextSelection = (listRef: RefObject<HTMLDivElement>, blurSearch: () => void) => {
  const [startSelectionPosition, setStartSelectionPosition] = useState<TextSelection | null>(null);
  const [endSelectionPosition, setEndSelectionPosition] = useState<TextSelection | null>(null);

  const selectionRef = useRef<{ start: TextSelection | null, end: TextSelection | null }>({ start: null, end: null });

  const mouseMoveHandler = useCallback((e: MouseEvent) => {
    e.preventDefault();
    const position = getMousePosition(e);
    if (!position) {
      return;
    }
    selectionRef.current.end = position;
    setEndSelectionPosition(position);
  }, []);

  /** Add listeners for text selection */
  useEffect(() => {
    const startSelection = (e: MouseEvent) => {
      if (e.button !== 0) return;

      if (!listRef.current) {
        return;
      }

      const position = getMousePosition(e);
      if (!position) {
        return;
      }
      e.preventDefault();
      blurSearch();

      if (e.shiftKey) {
        const newPos = getSelectionForShiftKey(selectionRef.current, position);
        selectionRef.current = newPos;
        setStartSelectionPosition(newPos.start);
        setEndSelectionPosition(newPos.end);
      } else {
        selectionRef.current = {
          start: position,
          end: null,
        };
        setEndSelectionPosition(null);
        setStartSelectionPosition(position);
        listRef.current.addEventListener("mousemove", mouseMoveHandler);
      }
    };

    const endSelection = (e: MouseEvent) => {
      if (e.button !== 0) return;
      e.preventDefault();
      if (!listRef.current) {
        return;
      }
      listRef.current.removeEventListener("mousemove", mouseMoveHandler);
    };

    const selectWord = (e: MouseEvent) => {
      if (e.button !== 0) return;
      e.preventDefault();
      if (!listRef.current) {
        return;
      }
      const position = getWordSelectionAtMouse(e);
      if (!position) {
        return;
      }
      e.preventDefault();
      blurSearch();
      selectionRef.current = position;
      setStartSelectionPosition(position.start);
      setEndSelectionPosition(position.end);
    };

    if (listRef.current) {
      listRef.current.addEventListener("mousedown", startSelection);
      listRef.current.addEventListener("mouseup", endSelection);
      listRef.current.addEventListener("dblclick", selectWord);
    }

    return () => {
      if (listRef.current) {
        listRef.current.removeEventListener("mousedown", startSelection);
        listRef.current.removeEventListener("mouseup", endSelection);
        listRef.current.removeEventListener("dblclick", selectWord);
      }
    };
  }, []);

  return { startSelectionPosition, endSelectionPosition, selectionRef };
};
