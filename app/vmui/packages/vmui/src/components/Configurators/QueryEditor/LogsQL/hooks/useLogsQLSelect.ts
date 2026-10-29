import { useCallback } from "preact/compat";
import { AutocompleteOptions } from "../../../../Main/Autocomplete/Autocomplete";
import { ContextData, ContextType, LogicalPart } from "../types";
import { STREAM_FIELD } from "../../../../../constants/logFields";
import { escapeRegexp } from "../../../../../utils/regexp";

type LogsQLSelectContext = ContextData & LogicalPart;

type UseLogsQLSelectArgs = {
  contextData?: LogsQLSelectContext;
  logicalParts: LogicalPart[];
  onSelect: (val: string, caretPosition: number, keepOpen?: boolean) => void;
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
    // The selected value is a literal, so escape it for regexp filters.
    const isRegexp = operator === ":~" || operator === ":!~";
    const literal = isRegexp ? escapeRegexp(modifiedInsert) : modifiedInsert;
    const insertWithQuotes = value.startsWith(`${STREAM_FIELD}:`)
      ? modifiedInsert
      : JSON.stringify(literal);

    modifiedInsert = `${filterName || ""}${operator || ":"}${insertWithQuotes}`;
  }

  const indentStart = value.match(/^\s+/)?.[0] ?? "";
  const indentEnd = value.match(/\s+$/)?.[0] ?? "";

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

    const partIndex = logicalParts.findIndex(part => part.id === contextData.id);
    const prefix = logicalParts
      .slice(0, partIndex)
      .map(part => `${part.separator ?? ""}${part.value}`)
      .join("");

    const updatedPosition = (
      prefix
      + (contextData.separator ?? "")
      + insertValue.trimEnd()
    ).trimStart().length;

    onSelect(newValue, updatedPosition, item.type === ContextType.FilterName);
  }, [contextData, logicalParts, onSelect]);
};
