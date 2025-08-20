import { useRef, useState, useEffect, useCallback, useMemo, FC } from "preact/compat";
import "./style.scss";
import { TextSelection } from "./types";
import {
  getCurrentFocusEntry,
  getSelectionData,
  getSelectionPosition
} from "./utils";
import { HighlightedText } from "./HighlightedText";
import { currentSearchFocusedElement } from "./constants";
import { useTextSelection } from "./hooks/useTextSelection";
import { useContextMenu } from "./hooks/useContextMenu";
import useBoolean from "../../../hooks/useBoolean";
import useCopyToClipboard from "../../../hooks/useCopyToClipboard";
import TextField, { TextFieldKeyboardEvent } from "../TextField/TextField";
import Popper from "../Popper/Popper";
import Button from "../Button/Button";

const getSelectionText = (
  text: string,
  elementIndex: number,
  startSelection: TextSelection | null,
  endSelection: TextSelection | null,
  currentSearchPosition: TextSelection | null,
  searchValue?: string
) => {
  const currentSearchPositionIndex =
    currentSearchPosition?.elementIndex === elementIndex
      ? currentSearchPosition.positionIndex
      : undefined;
  if ((!startSelection || !endSelection)) {
    if (searchValue && searchValue.length > 0) {
      return (
        <HighlightedText
          text={text}
          searchValue={searchValue}
          currentSearchPositionIndex={currentSearchPositionIndex}
        />
      );
    } else {
      return text;
    }
  }

  const { start, end } = getSelectionPosition(startSelection, endSelection);
  if (start.elementIndex > elementIndex || end.elementIndex < elementIndex) {
    if (searchValue && searchValue.length > 0) {
      return (
        <HighlightedText
          text={text}
          searchValue={searchValue}
          currentSearchPositionIndex={currentSearchPositionIndex}
        />
      );
    } else {
      return text;
    }
  }

  let startPos = 0;
  let endPos = text.length;
  if (start.elementIndex === end.elementIndex) {
    startPos = start.positionIndex;
    endPos = end.positionIndex;
  } else if (start.elementIndex === elementIndex) {
    startPos = start.positionIndex;
  } else if (end.elementIndex === elementIndex) {
    endPos = end.positionIndex;
  }

  return (
    <HighlightedText
      text={text}
      searchValue={searchValue}
      selectionStart={startPos}
      selectionEnd={endPos}
      currentSearchPositionIndex={currentSearchPositionIndex}
    />
  );
};

interface Props {
  data: string[];
  elementHeight?: number;
}

/**
 * This component optimizes rendering performance by only rendering visible items within the viewport
 * and dynamically calculating the visible range based on the current document scroll position.
 * Provides features such as text selection, search, and copy-to-clipboard.
 */
export const DocumentVirtualizedList: FC<Props> = ({
  data,
  elementHeight = 16,
}) => {
  // buffer of elements that should be rendered outside the visible area
  const elementOverhead = useMemo(() => Math.ceil(window.innerHeight / elementHeight), [elementHeight]);

  const listRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLDivElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);

  const [visibleItems, setVisibleItems] = useState({
    startIndex: 0,
    endIndex: Math.ceil(window.innerHeight / elementHeight) + elementOverhead,
  });
  const [searchValue, setSearchValue] = useState<string>("");
  const { value: isFixedSearch, setValue: setIsFixedSearch } = useBoolean(false);
  const [currentSearchFocusPosition, setCurrentSearchFocusPosition] = useState<TextSelection | null>(null);
  const { value: isSearchOpen, setValue: setIsSearchOpen } = useBoolean(false);

  const blurSearch = () => {
    searchInputRef.current?.blur();
  };
  const { startSelectionPosition, endSelectionPosition, selectionRef, setSelection } = useTextSelection(listRef, blurSearch);
  const { contextMenu, handleCloseContextMenu } = useContextMenu({ startSelectionPosition, endSelectionPosition, listRef, setSelection });

  const copyToClipboard = useCopyToClipboard();
  const itemsCount = data.length;

  const onSearchKeyDown = (e: TextFieldKeyboardEvent) => {
    if (e.key !== "Enter" && e.key !== "ArrowDown" && e.key !== "ArrowUp") {
      return;
    }
    e.preventDefault();

    const forward = e.key !== "ArrowUp";
    const currentFocusPosition = getCurrentFocusEntry(data, searchValue, currentSearchFocusPosition, forward);
    setCurrentSearchFocusPosition(currentFocusPosition);
    if (!currentFocusPosition) {
      return;
    }

    if (currentFocusPosition.elementIndex < visibleItems.startIndex || currentFocusPosition.elementIndex > visibleItems.endIndex) {
      const newStartIndex = Math.max(0, currentFocusPosition.elementIndex - elementOverhead);
      const newEndIndex = Math.min(itemsCount, newStartIndex + visibleItems.endIndex - visibleItems.startIndex);
      setVisibleItems({
        startIndex: newStartIndex,
        endIndex: newEndIndex,
      });
    }
  };

  const handleSearchChange = useCallback((value: string) => {
    const currentFocusPosition = getCurrentFocusEntry(data, value, null, true);
    setCurrentSearchFocusPosition(currentFocusPosition);
    setSearchValue(value);
  }, [data]);

  const handleContextMenuCopy = useCallback(() => {
    if (startSelectionPosition && endSelectionPosition) {
      const selectedData = getSelectionData(data, startSelectionPosition, endSelectionPosition);
      copyToClipboard(selectedData, "Copied to clipboard");
    }
  }, [data, startSelectionPosition, endSelectionPosition, copyToClipboard]);

  /** Scrolling to the current search position */
  useEffect(() => {
    if (!currentSearchFocusPosition) {
      return;
    }

    if (listRef.current) {
      const el = document.getElementById(currentSearchFocusedElement);
      if (el) {
        el.scrollIntoView({ block: "center", inline: "nearest" });
      }
    }
  }, [currentSearchFocusPosition]);

  /** Add listener for scroll to calculate visible items */
  useEffect(() => {
    const handleScroll = () => {
      if (listRef.current) {
        const rect = listRef.current.getBoundingClientRect();
        const viewportHeight = window.innerHeight;

        const visibleTop = Math.max(rect?.top || 0, 0);
        const visibleBottom = Math.min(rect?.bottom || 0, viewportHeight);
        const visibleHeight = Math.max(visibleBottom - visibleTop, 0);
        let visibleElementStartIndex = 0;
        if (rect.top <= 0) {
          visibleElementStartIndex = Math.floor(Math.abs(rect.top) / elementHeight);
        }

        const visibleElementEndIndex = visibleElementStartIndex + Math.ceil(visibleHeight / elementHeight);
        setVisibleItems({
          startIndex: visibleElementStartIndex - elementOverhead > 0 ? visibleElementStartIndex - elementOverhead : visibleElementStartIndex,
          endIndex: visibleElementEndIndex + elementOverhead < itemsCount ? visibleElementEndIndex + elementOverhead : visibleElementEndIndex,
        });

        if (rect.top <= 50) {
          setIsFixedSearch(true);
        } else if (rect.top > 50) {
          setIsFixedSearch(false);
        }
      }
    };

    document.addEventListener("scroll", handleScroll);
    return () => {
      document.removeEventListener("scroll", handleScroll);
    };
  }, []);

  /** Add listener for search hotkey */
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        console.log("Escape");
        setIsSearchOpen(false);
        setSearchValue("");
        handleCloseContextMenu();
      }
    };

    const handleSearch = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "f") {
        e.preventDefault();
        let newSearchValue = "";
        if (selectionRef.current.start && selectionRef.current.end) {
          newSearchValue = getSelectionData(data, selectionRef.current.start, selectionRef.current.end);
        } else if (isSearchOpen && searchInputRef.current) {
          searchInputRef.current.focus?.();
          return;
        }
        setSearchValue(newSearchValue);
        setIsSearchOpen(true);
        searchInputRef.current?.focus?.();
      }
    };

    window.addEventListener("keydown", handleSearch);
    window.addEventListener("keydown", handleEscape, true);
    searchRef.current?.addEventListener("keydown", handleEscape, true);
    return () => {
      window.removeEventListener("keydown", handleSearch);
      window.removeEventListener("keydown", handleEscape, true);
      searchRef.current?.removeEventListener("keydown", handleEscape, true);
    };
  }, [data, handleCloseContextMenu, isSearchOpen]);

  /** Add listener for copy to clipboard hotkeys */
  useEffect(() => {
    if (!endSelectionPosition) {
      return;
    }

    const handleCopy = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "c" && startSelectionPosition && endSelectionPosition) {
        e.preventDefault();
        const selectedData = getSelectionData(data, startSelectionPosition, endSelectionPosition);
        copyToClipboard(selectedData, "Copied to clipboard");
      }
    };
    window.addEventListener("keydown", handleCopy);
    return () => {
      window.removeEventListener("keydown", handleCopy);
    };
  }, [endSelectionPosition]);

  const marginBottom = itemsCount * elementHeight - visibleItems.endIndex * elementHeight;
  const marginTop = visibleItems.startIndex * elementHeight;

  return (
    <>
      <div
        className="vm-document-virtualized-list"
        ref={listRef}
        style={{ paddingBottom: marginBottom, paddingTop: marginTop }}
      >
        {data.slice(visibleItems.startIndex, visibleItems.endIndex).map((item, idx) => <pre
          style={{ lineHeight: `${elementHeight}px` }}
          data-id={idx + visibleItems.startIndex}
          key={idx + visibleItems.startIndex}
        >{getSelectionText(item, idx + visibleItems.startIndex, startSelectionPosition, endSelectionPosition, currentSearchFocusPosition, searchValue)}</pre>)}
      </div>
      {isSearchOpen &&
        <div
          style={{ position: isFixedSearch ? "fixed" : "absolute" }}
          ref={searchRef}
          className="vm-document-virtualized-list__search"
        >
          <TextField
            ref={searchInputRef}
            value={searchValue}
            onChange={handleSearchChange}
            onKeyDown={onSearchKeyDown}
            autofocus
            type={"text"}
          />
        </div>
      }
      <Popper
        open={contextMenu.isVisible}
        placementPosition={{ top: contextMenu.y, left: contextMenu.x }}
        placement={"fixed"}
        onClose={handleCloseContextMenu}
        buttonRef={listRef}
      >
        <Button
          onClick={handleContextMenuCopy}
          variant="text"
        >
          Copy
        </Button>
      </Popper>
    </>
  );
};
