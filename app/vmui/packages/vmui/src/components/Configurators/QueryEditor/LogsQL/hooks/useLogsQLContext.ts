import { useMemo } from "preact/compat";
import { getContextData, splitLogicalParts } from "../helpers/parser";

type UseLogsQLContextArgs = {
  value: string;
  caretPosition: [number, number];
};

export const useLogsQLContext = ({
  value,
  caretPosition,
}: UseLogsQLContextArgs) => {
  const [selectionStart, selectionEnd] = caretPosition;
  const hasSelection = selectionStart !== selectionEnd;

  const logicalParts = useMemo(() => {
    return splitLogicalParts(value);
  }, [value]);

  const contextData = useMemo(() => {
    if (hasSelection) return;

    const partIndex = logicalParts.findIndex(p => {
      return selectionStart >= p.position[0] && selectionStart <= p.position[1];
    });

    if (partIndex === -1) return;

    const part = logicalParts[partIndex];
    const cursorStartPosition = selectionStart - part.position[0];
    const prevPart = logicalParts[partIndex - 1];

    const queryBeforeIncompleteFilter = prevPart
      ? value.substring(0, prevPart.position[1] + 1)
      : undefined;

    return {
      ...part,
      queryBeforeIncompleteFilter,
      query: value,
      ...getContextData(part, cursorStartPosition),
    };
  }, [logicalParts, selectionStart, hasSelection, value]);

  return {
    logicalParts,
    contextData,
  };
};
