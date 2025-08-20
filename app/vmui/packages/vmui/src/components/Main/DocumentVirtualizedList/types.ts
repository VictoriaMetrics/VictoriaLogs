export type TextSelection = {
  /** Index of item */
  elementIndex: number;
  /** Index of symbol in item */
  positionIndex: number;
}

export type TextSelectionRange = {
  /** Start position of selection */
  start: number;
  /** End position of selection */
  end: number;
  /** Type of selection */
  type: "search" | "selection";
}

export type TextFragment = {
  /** Text fragment */
  text: string;
  /** Start position of fragment */
  start: number;
  /** End position of fragment */
  end: number;
  /** Highlight type */
  highlight: "search" | "selection" | "both" | null;
}
