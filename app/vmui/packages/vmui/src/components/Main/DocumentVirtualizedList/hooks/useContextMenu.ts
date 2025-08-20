import { useCallback, useEffect, useState, RefObject } from "preact/compat";
import { TextSelection } from "../types";
import { getMousePosition, getWordSelectionAtMouse } from "./utils";
import { isClickInSelection } from "./useContextMenuUtils";

interface UseContextMenuProps {
  startSelectionPosition: TextSelection | null;
  endSelectionPosition: TextSelection | null;
  listRef: RefObject<HTMLDivElement>;
  setSelection: (start: TextSelection | null, end: TextSelection | null) => void;
}

interface ContextMenuState {
  isVisible: boolean;
  x: number;
  y: number;
}

/**
 * Custom hook that provides context menu functionality for a text selection or an interactive list.
 * It manages the visibility and position of the context menu based on user interactions such as right-click events.
 *
 * @param {Object} params Object containing properties to configure the context menu behavior.
 * @param {Object} params.startSelectionPosition The starting position of the text selection. Used to determine if the click is within the selection.
 * @param {Object} params.endSelectionPosition The ending position of the text selection. Used to determine if the click is within the selection.
 * @param {React.RefObject} params.listRef A React reference to the list or container element where the context menu should be active.
 * @param {Function} params.setSelection Function to set the start and end positions of the selection, typically used to highlight text.
 * @returns {Object} Returns an object containing the current context menu state and a function to close the menu.
 * @property {Object} contextMenu State object describing the visibility and position of the context menu.
 * @property {boolean} contextMenu.isVisible Indicates whether the context menu is currently visible.
 * @property {number} contextMenu.x The x-coordinate of the context menu's position.
 * @property {number} contextMenu.y The y-coordinate of the context menu's position.
 * @property {Function} handleCloseContextMenu Function to close the context menu by resetting its state.
 */
export const useContextMenu = ({
  startSelectionPosition,
  endSelectionPosition,
  listRef,
  setSelection
}: UseContextMenuProps): {
  contextMenu: ContextMenuState,
  handleCloseContextMenu: () => void
} => {
  const [contextMenu, setContextMenu] = useState<ContextMenuState>({ isVisible: false, x: 0, y: 0 });

  const handleContextMenu = useCallback((e: MouseEvent) => {
    e.preventDefault();

    const clickPosition = getMousePosition(e);
    if (!clickPosition) return;

    // If there is a selection, check if the click is within it
    if (startSelectionPosition && endSelectionPosition) {
      if (isClickInSelection(clickPosition, startSelectionPosition, endSelectionPosition)) {
        // Click on a selected area - show context menu
        setContextMenu({
          isVisible: true,
          x: e.clientX,
          y: e.clientY,
        });
        return;
      }
    }

    // No selection - select word under the cursor
    const wordSelection = getWordSelectionAtMouse(e);
    if (wordSelection) {
      setSelection(wordSelection.start, wordSelection.end);
      // Show a context menu for a new selection
      setContextMenu({
        isVisible: true,
        x: e.clientX,
        y: e.clientY,
      });
    }
  }, [startSelectionPosition, endSelectionPosition, isClickInSelection, setSelection]);

  const handleCloseContextMenu = useCallback(() => {
    setContextMenu({ isVisible: false, x: 0, y: 0 });
  }, []);

  /** Add context menu listeners */
  useEffect(() => {
    if (listRef.current) {
      listRef.current.addEventListener("contextmenu", handleContextMenu);
    }

    // Close the context menu on click anywhere
    const handleClick = () => {
      handleCloseContextMenu();
    };

    document.addEventListener("click", handleClick);

    return () => {
      if (listRef.current) {
        listRef.current.removeEventListener("contextmenu", handleContextMenu);
      }
      document.removeEventListener("click", handleClick);
    };
  }, [handleContextMenu, handleCloseContextMenu]);

  return {
    contextMenu,
    handleCloseContextMenu,
  };
};
