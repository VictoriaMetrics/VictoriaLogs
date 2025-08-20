import { memo, CSSProperties } from "preact/compat";
import { TextSelectionRange } from "./types";
import { getOverlappedFragments } from "./utils";
import { currentSearchFocusedElement } from "./constants";

const highlightColor = "#469DBD";
const searchColor = "#BDB300";
const focusColor = "#DF7700";

type HighlightedTextProps = {
  /** The main text string that will be displayed and potentially highlighted */
  text: string;
  /** An optional string used for searching and highlighting within the text */
  searchValue?: string;
  /** An optional number representing the starting index of the selected text */
  selectionStart?: number;
  /** An optional number representing the ending index of the selected text */
  selectionEnd?: number;
  /** Used for highlight search navigation */
  currentSearchPositionIndex?: number;
};

export const HighlightedText = memo(({
  text,
  searchValue = "",
  selectionStart,
  selectionEnd,
  currentSearchPositionIndex
}: HighlightedTextProps) => {
  const lowerText = text.toLowerCase();
  const lowerSearch = searchValue.toLowerCase();
  const matches: TextSelectionRange[] = [];

  if (searchValue) {
    let index = 0;
    while ((index = lowerText.indexOf(lowerSearch, index)) !== -1) {
      matches.push({ start: index, end: index + searchValue.length, type: "search" });
      index += searchValue.length;
    }
  }

  let selection: TextSelectionRange | undefined;
  if (selectionStart !== selectionEnd && selectionStart !== undefined && selectionEnd !== undefined) {
    selection = { start: selectionStart, end: selectionEnd, type: "selection" };
  }

  const fragments = getOverlappedFragments(text, matches, selection);
  return (
    <span>
      {fragments.map((frag, idx) => {
        let style: CSSProperties = {};
        if (frag.highlight === "search") style = { backgroundColor: searchColor };
        else if (frag.highlight === "selection") style = { backgroundColor: highlightColor };
        else if (frag.highlight === "both") style = { backgroundColor: highlightColor, color: searchColor };

        let id;
        if (frag.start === currentSearchPositionIndex) {
          if (frag.highlight === "both") {
            style.color = focusColor;
          } else {
            style.backgroundColor = focusColor;
          }
          id = currentSearchFocusedElement;
        }

        return (
          <span
            key={idx}
            style={style}
            id={id}
          >
            {frag.text}
          </span>
        );
      })}
    </span>
  );
});

