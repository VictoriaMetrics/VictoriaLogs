import { useCallback } from "preact/compat";
import { AutocompleteOptions } from "../../../../Main/Autocomplete/Autocomplete";
import { ContextData, ContextType, LogicalPart } from "../types";

type LogsQLSelectContext = ContextData & LogicalPart;

type UseLogsQLSelectArgs = {
  contextData?: LogsQLSelectContext;
  logicalParts: LogicalPart[];
  onSelect: (val: string, caretPosition: number) => void;
};

const shouldReplaceWholeInput = (type?: string) => {
  return type === ContextType.Example || type === ContextType.History;
};

const getUpdatedValue = (
  insertValue: string,
  logicalParts: LogicalPart[],
  id?: number,
) => {
  return logicalParts.reduce((acc, part) => {
    const value = part.id === id ? insertValue : part.value;
    return `${acc}${part.separator}${value}`;
  }, "").trim();
};

const getModifyInsert = (
  insert: string,
  contextData: LogsQLSelectContext,
  insertType?: string
) => {
  let modifiedInsert = insert;
  const { contextType, value, filterName, operator } = contextData;

  if (insertType === ContextType.FilterName) {
    modifiedInsert += ":";
  } else if (contextType === ContextType.FilterValue) {
    const insertWithQuotes = value.startsWith("_stream:")
      ? modifiedInsert
      : `${JSON.stringify(modifiedInsert)}`;

    modifiedInsert = `${filterName || ""}${operator || ":"}${insertWithQuotes}`;
  }

  const indentStart = value.match(/^[ \t]+/)?.[0] ?? "";
  const indentEnd = value.match(/[ \t]+$/)?.[0] ?? "";

  return `${indentStart}${modifiedInsert}${indentEnd}`;
};

export const useLogsQLSelect = ({
  contextData,
  logicalParts,
  onSelect,
}: UseLogsQLSelectArgs) => {
  return useCallback((insert: string, item: AutocompleteOptions) => {
    if (shouldReplaceWholeInput(item.type)) {
      onSelect(insert, insert.length);
      return;
    }

    if (!contextData) return;

    const insertValue = getModifyInsert(insert, contextData, item.type);
    const newValue = getUpdatedValue(insertValue, logicalParts, contextData.id);

    const updatedPosition =
      (contextData.position[0] || 1) + insertValue.trim().length;

    onSelect(newValue, updatedPosition);
  }, [contextData, logicalParts, onSelect]);
};
