import { useEffect, useState, useRef, RefObject, useCallback } from "preact/compat";
import { TextSelection } from "../types";
import { getSelectionForShiftKey } from "../utils";
import { getMousePosition, getWordSelectionAtMouse } from "./utils";

/**
 * Custom hook for handling text selection functionality in a virtualized list component.
 * Supports single click, double click (word selection), triple click (line selection),
 * drag selection with auto-scroll, and Shift+click for extended selection.
 *
 * @param listRef - Reference to the container element containing selectable text
 * @param blurSearch - Callback function to blur the search input when selection starts
 * @returns Object containing selection state and utilities
 */
export const useTextSelection = (listRef: RefObject<HTMLDivElement>, blurSearch: () => void) => {
  // Current selection boundaries
  const [startSelectionPosition, setStartSelectionPosition] = useState<TextSelection | null>(null);
  const [endSelectionPosition, setEndSelectionPosition] = useState<TextSelection | null>(null);

  // Auto-scroll timer for drag selection near viewport edges
  const autoScrollTimer = useRef<NodeJS.Timeout | null>(null);

  // Click tracking for multi-click detection (double/triple click)
  const clickCount = useRef<number>(0);
  const clickTimer = useRef<NodeJS.Timeout | null>(null);

  // Internal reference to the current selection state for event handlers
  const selectionRef = useRef<{ start: TextSelection | null, end: TextSelection | null }>({ start: null, end: null });

  /**
   * Gets line selection boundaries for triple-click functionality.
   * Selects entire line content from beginning to end.
   */
  const getLineSelectionAtMouse = useCallback((e: MouseEvent): { start: TextSelection, end: TextSelection } | null => {
    const target = e.target as HTMLElement;
    if (!target) return null;

    // Find any element that has a data-id attribute
    const containerElement = target.closest("[data-id]");
    if (!containerElement) return null;

    // Get element index from data-id attribute
    const elementIndex = parseInt(containerElement.getAttribute("data-id") || "0", 10);

    // Return selection for the entire line (from start to end)
    return {
      start: {
        elementIndex,
        positionIndex: 0
      },
      end: {
        elementIndex,
        positionIndex: containerElement.textContent?.length || 0
      }
    };
  }, []);

  /**
   * Handles auto-scrolling when drag selection approaches viewport edges.
   * Scrolls both horizontally (within container) and vertically (window).
   */
  const autoScroll = useCallback((verticalDirection?: "up" | "down", horizontalDirection?: "left" | "right") => {
    const scrollAmount = 50; // Pixels to scroll per interval

    let verticalScroll = 0;
    let horizontalScroll = 0;

    // Calculate scroll amounts based on a direction
    if (verticalDirection === "up") {
      verticalScroll = -scrollAmount;
    } else if (verticalDirection === "down") {
      verticalScroll = scrollAmount;
    }

    if (horizontalDirection === "left") {
      horizontalScroll = -scrollAmount;
    } else if (horizontalDirection === "right") {
      horizontalScroll = scrollAmount;
    }

    // Apply scrolling: horizontal to container, vertical to window
    if (listRef.current && horizontalScroll !== 0) {
      listRef.current.scrollBy(horizontalScroll, 0);
    }

    if (verticalScroll !== 0) {
      window.scrollBy(0, verticalScroll);
    }
  }, [listRef]);

  /**
   * Handles mouse move events during drag selection.
   * Updates selection end position and manages auto-scroll when near viewport edges.
   */
  const mouseMoveHandler = useCallback((e: MouseEvent) => {
    e.preventDefault();

    // Update selection end position
    const position = getMousePosition(e);
    if (!position) {
      return;
    }
    selectionRef.current.end = position;
    setEndSelectionPosition(position);

    // Auto-scroll configuration
    const scrollThreshold = 50; // Distance from edge to trigger auto-scroll
    const scrollSpeed = 100; // Auto-scroll interval in milliseconds

    const viewportHeight = window.innerHeight;
    const viewportWidth = window.innerWidth;
    const mouseY = e.clientY;
    const mouseX = e.clientX;

    // Clear the previous auto-scroll timer
    if (autoScrollTimer.current) {
      clearInterval(autoScrollTimer.current);
      autoScrollTimer.current = null;
    }

    // Determine scroll directions based on mouse position
    let verticalDirection: "up" | "down" | undefined;
    let horizontalDirection: "left" | "right" | undefined;

    // Check vertical scroll (relative to viewport)
    if (mouseY < scrollThreshold) {
      verticalDirection = "up";
    } else if (mouseY > viewportHeight - scrollThreshold) {
      verticalDirection = "down";
    }

    // Check horizontal scroll (relative to viewport)
    if (mouseX < scrollThreshold) {
      horizontalDirection = "left";
    } else if (mouseX > viewportWidth - scrollThreshold) {
      horizontalDirection = "right";
    }

    // Start auto-scroll if needed
    if (verticalDirection || horizontalDirection) {
      autoScrollTimer.current = setInterval(() => {
        autoScroll(verticalDirection, horizontalDirection);
      }, scrollSpeed);
    }
  }, [autoScroll]);

  /**
   * Updates both internal ref and state for selection boundaries.
   * Used for programmatic selection updates.
   */
  const setSelection = useCallback((start: TextSelection | null, end: TextSelection | null) => {
    selectionRef.current.start = start;
    selectionRef.current.end = end;
    setStartSelectionPosition(start);
    setEndSelectionPosition(end);
  }, []);

  /**
   * Main effect that sets up all mouse event listeners for text selection functionality.
   * Handles mousedown (start selection), mouseup (end selection), and dblclick (word selection).
   */
  useEffect(() => {
    /**
     * Handles word selection on double-click.
     * Selects the entire word under the mouse cursor.
     */
    const selectWord = (e: MouseEvent) => {
      // Only handle the left mouse button
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

    /**
     * Initiates text selection on mouse down.
     * Handles single, double, and triple-click scenarios.
     */
    const startSelection = (e: MouseEvent) => {
      // Only handle the left mouse button
      if (e.button !== 0) return;

      if (!listRef.current) {
        return;
      }

      // Track click count for multi-click detection
      clickCount.current += 1;

      // Clear the previous click timer: need it for detecting triple-clicks
      if (clickTimer.current) {
        clearTimeout(clickTimer.current);
      }

      // Set the timer to reset click count (400ms window for triple-click detection)
      clickTimer.current = setTimeout(() => {
        clickCount.current = 0;
      }, 400);

      const position = getMousePosition(e);
      if (!position) {
        return;
      }
      e.preventDefault();
      blurSearch();

      /*
      * Handle double-click: select the word entry
      * Not use 'dblclick' event because it doesn't work well with 'triple click' handlers
      * */
      if (clickCount.current === 2) {
        selectWord(e);
        return;
      }

      // Handle triple-click: select the entire line
      if (clickCount.current === 3) {
        const lineSelection = getLineSelectionAtMouse(e);
        if (lineSelection) {
          selectionRef.current = lineSelection;
          setStartSelectionPosition(lineSelection.start);
          setEndSelectionPosition(lineSelection.end);
        }
        clickCount.current = 0;
        return;
      }

      // Handle Shift+click: extend the existing selection
      if (e.shiftKey) {
        const newPos = getSelectionForShiftKey(selectionRef.current, position);
        selectionRef.current = newPos;
        setStartSelectionPosition(newPos.start);
        setEndSelectionPosition(newPos.end);
      } else {
        // Start a new selection
        selectionRef.current = {
          start: position,
          end: null,
        };
        setEndSelectionPosition(null);
        setStartSelectionPosition(position);

        // Add global mouse move listener for drag selection
        document.addEventListener("mousemove", mouseMoveHandler);
      }
    };

    /**
     * Ends text selection on mouse up.
     * Cleans up event listeners and stops auto-scroll.
     */
    const endSelection = (e: MouseEvent) => {
      // Only handle the left mouse button
      if (e.button !== 0) return;
      e.preventDefault();

      if (autoScrollTimer.current) {
        clearInterval(autoScrollTimer.current);
        autoScrollTimer.current = null;
      }

      document.removeEventListener("mousemove", mouseMoveHandler);
    };

    if (listRef.current) {
      listRef.current.addEventListener("mousedown", startSelection);
      document.addEventListener("mouseup", endSelection);
    }

    return () => {
      if (autoScrollTimer.current) {
        clearInterval(autoScrollTimer.current);
      }
      if (clickTimer.current) {
        clearTimeout(clickTimer.current);
      }

      document.removeEventListener("mousemove", mouseMoveHandler);
      document.removeEventListener("mouseup", endSelection);

      if (listRef.current) {
        listRef.current.removeEventListener("mousedown", startSelection);
      }
    };
  }, [mouseMoveHandler, getLineSelectionAtMouse]);

  return {
    startSelectionPosition,
    endSelectionPosition,
    selectionRef,
    setSelection
  };
};
