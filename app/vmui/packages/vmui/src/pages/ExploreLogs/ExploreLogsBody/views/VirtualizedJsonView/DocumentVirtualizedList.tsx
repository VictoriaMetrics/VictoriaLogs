import { useRef, useState, useEffect, useCallback, useMemo, FC } from "preact/compat";
import "./style.scss";
import TextField from "../../../../../components/Main/TextField/TextField";
import useBoolean from "../../../../../hooks/useBoolean";
import { TextSelection } from "./types";
import { findAllMatches, getMousePosition, getSelectionData, getSelectionPosition } from "./utils";
import { HighlightedText } from "./HighlightedText";
import useCopyToClipboard from "../../../../../hooks/useCopyToClipboard";
import { currentSearchFocusedElement } from "./constants";

const getSelectionText = (
  text: string,
  elementIndex: number,
  startSelection: TextSelection | null,
  endSelection: TextSelection | null,
  searchValue?: string,
  currentSearchPosition?: TextSelection
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
 * Provides features such as text selection, search, and copy-to-clipboard for JSON data.
 */
export const DocumentVirtualizedList: FC<Props> = ({
  data,
  elementHeight = 16,
}) => {
  const elementOverhead = useMemo(() => Math.ceil(window.innerHeight / elementHeight), [elementHeight]);

  const listRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLDivElement>(null);
  const preVisibleHeightRef = useRef<number>(0);

  const [visibleItems, setVisibleItems] = useState({
    startIndex: 0,
    endIndex: Math.ceil(window.innerHeight / elementHeight) + elementOverhead,
  });
  const [startSelection, setStartSelection] = useState<TextSelection | null>(null);
  const [endSelection, setEndSelection] = useState<TextSelection | null>(null);
  const [searchValue, setSearchValue] = useState<string>("");
  const { value: isFixedSearch, setValue: setIsFixedSearch } = useBoolean(false);
  const [currentSearchPos, setCurrentSearchPos] = useState<number>(0);
  const [searchMatches, setSearchMatches] = useState<TextSelection[]>([]);
  const { value: isSearchOpen, setValue: setIsSearchOpen } = useBoolean(false);
  const copyToClipboard = useCopyToClipboard();
  const itemsCount = data.length;

  const mouseMoveHandler = useCallback((e: MouseEvent) => {
    e.preventDefault();
    const position = getMousePosition(e);
    if (!position) {
      return;
    }
    setEndSelection(position);
  }, []);

  const onSearchKeyDown = () => {
    const newSearchPos = currentSearchPos + 1 % searchMatches.length;
    setCurrentSearchPos(newSearchPos);

    if (!searchMatches[newSearchPos]) {
      return;
    }

    if (searchMatches[newSearchPos].elementIndex < visibleItems.startIndex || searchMatches[currentSearchPos].elementIndex > visibleItems.endIndex) {
      const newStartIndex = Math.max(0, searchMatches[currentSearchPos].elementIndex - elementOverhead);
      const newEndIndex = Math.min(itemsCount, newStartIndex + visibleItems.endIndex - visibleItems.startIndex);
      setVisibleItems({
        startIndex: newStartIndex,
        endIndex: newEndIndex,
      });
    }
  };

  const handleSearchChange = useCallback((value: string) => {
    const allMatches = findAllMatches(data, value);
    setCurrentSearchPos(0);
    setSearchMatches(allMatches);
    setSearchValue(value);
  }, [data]);

  /** Scrolling to the current search position */
  useEffect(() => {
    if (!searchMatches[currentSearchPos]) {
      return;
    }

    if (listRef.current) {
      const el = document.getElementById(currentSearchFocusedElement);
      if (el) {
        el.scrollIntoView({ block: "center", inline: "nearest" });
      }
    }
  }, [searchMatches, currentSearchPos]);


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
        if (rect.top > 0) {
          preVisibleHeightRef.current = visibleHeight;
        } else {
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
    const handleSearch = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "f") {
        e.preventDefault();
        setIsSearchOpen(true);
      }
      if (e.key === "Escape") {
        setIsSearchOpen(false);
        setSearchValue("");
      }
    };

    window.addEventListener("keydown", handleSearch);
    return () => {
      window.removeEventListener("keydown", handleSearch);
    };
  });

  /** Add listeners for text selection */
  useEffect(() => {
    const startSelection = (e: MouseEvent) => {
      if (e.button !== 0) return;

      e.preventDefault();
      setEndSelection(null);
      if (!listRef.current) {
        return;
      }

      const position = getMousePosition(e);
      if (!position) {
        return;
      }
      setStartSelection(position);

      listRef.current.addEventListener("mousemove", mouseMoveHandler);
    };

    const endSelection = (e: MouseEvent) => {
      if (e.button !== 0) return;
      e.preventDefault();
      if (!listRef.current) {
        return;
      }
      listRef.current.removeEventListener("mousemove", mouseMoveHandler);
    };

    if (listRef.current) {
      listRef.current.addEventListener("mousedown", startSelection);
      listRef.current.addEventListener("mouseup", endSelection);
    }

    return () => {
      if (listRef.current) {
        listRef.current.removeEventListener("mousedown", startSelection);
        listRef.current.removeEventListener("mouseup", endSelection);
      }
    };
  }, []);

  /** Add listener for copy to clipboard hotkeys */
  useEffect(() => {
    if (!endSelection) {
      return;
    }

    const handleCopy = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "c" && startSelection && endSelection) {
        e.preventDefault();
        const selectedData = getSelectionData(data, startSelection, endSelection);
        copyToClipboard(selectedData, "Selected JSON data copied to clipboard");
      }
    };
    window.addEventListener("keydown", handleCopy);
    return () => {
      window.removeEventListener("keydown", handleCopy);
    };
  }, [endSelection]);

  const marginBottom = itemsCount * elementHeight - visibleItems.endIndex * elementHeight;
  const marginTop = visibleItems.startIndex * elementHeight;

  return (
    <div
      className="vm-json-virtualized-list"
    >
      <div
        ref={listRef}
        style={{ paddingBottom: marginBottom, paddingTop: marginTop }}
      >
        {data.slice(visibleItems.startIndex, visibleItems.endIndex).map((item, idx) => <pre
          style={{ lineHeight: `${elementHeight}px` }}
          data-id={idx + visibleItems.startIndex}
          key={idx + visibleItems.startIndex}
        >{getSelectionText(item, idx + visibleItems.startIndex, startSelection, endSelection, searchValue, searchMatches[currentSearchPos])}</pre>)}
      </div>
      {isSearchOpen &&
        <div
          style={{ position: isFixedSearch ? "fixed" : "absolute" }}
          ref={searchRef}
          className="vm-json-virtualized-list__search"
        >
          <TextField
            value={searchValue}
            onChange={handleSearchChange}
            onKeyDown={onSearchKeyDown}
            autofocus
            inputmode={"search"}
          />
        </div>
      }
    </div>
  );
};
