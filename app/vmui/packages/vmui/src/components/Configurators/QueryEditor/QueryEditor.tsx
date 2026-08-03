import { FC, useRef, useState, RefObject, useEffect } from "preact/compat";
import { ErrorTypes } from "../../../types";
import TextField, { TextFieldKeyboardEvent } from "../../Main/TextField/TextField";
import "./style.scss";
import { QueryStats } from "../../../api/types";
import { useQueryState } from "../../../state/query/QueryStateContext";
import { toggleLineComment } from "./LogsQL/helpers/utils";
import QueryEditorHotkeysTip from "./QueryEditorHotkeysTip";
import { formatRequestDuration } from "../../../utils/time";
import useBoolean from "../../../hooks/useBoolean";
import { DEFAULT_QUERY } from "../../../pages/QueryPage/hooks/useQueryController";

export interface QueryEditorAutocompleteProps {
  value: string;
  anchorEl: RefObject<HTMLElement>;
  caretPosition: [number, number]; // [start, end]
  onSelect: (val: string, caretPosition: number) => void;
}

export interface QueryEditorProps {
  onChange: (query: string) => void;
  onEnter: () => void;
  onArrowUp: () => void;
  onArrowDown: () => void;
  value: string;
  autocompleteEl?: FC<QueryEditorAutocompleteProps>;
  error?: ErrorTypes | string;
  stats?: QueryStats;
  label: string;
  disabled?: boolean
}

const QueryEditor: FC<QueryEditorProps> = ({
  value,
  onChange,
  onEnter,
  onArrowUp,
  onArrowDown,
  autocompleteEl: AutocompleteEl,
  error,
  stats,
  label,
  disabled = false,
}) => {
  const { autocomplete, autocompleteQuick } = useQueryState();

  const isDefaultQuery = value === "" || value === DEFAULT_QUERY;
  const isShowAutocomplete = autocompleteQuick || autocomplete || isDefaultQuery;

  const {
    value: isFocused,
    setTrue: onFocused,
    setFalse: onBlurred,
  } = useBoolean(false);

  const [autocompleteDismissed, setAutocompleteDismissed] = useState(false);
  const [caretPositionAutocomplete, setCaretPositionAutocomplete] = useState<[number, number]>([0, 0]);
  const [caretPositionInput, setCaretPositionInput] = useState<[number, number]>([value.length, value.length]);
  const autocompleteAnchorEl = useRef<HTMLDivElement>(null);

  const executionTimeMs = stats?.executionTimeMs;
  const labelPostfix = executionTimeMs ? ` (${formatRequestDuration(executionTimeMs)})` : "";

  const handleSelect = (val: string, caretPosition: number) => {
    onChange(val);
    setCaretPositionInput([caretPosition, caretPosition]);
    setAutocompleteDismissed(true);
  };

  const handleChange = (val: string) => {
    onChange(val);
    setAutocompleteDismissed(false);
  };

  const handleChangeCaret = (val: [number, number]) => {
    setCaretPositionAutocomplete(prev => prev[0] === val[0] && prev[1] === val[1] ? prev : val);
  };

  const handleFocus = () => {
    onFocused();
    setAutocompleteDismissed(false);
  };

  const handleKeyDown = (e: TextFieldKeyboardEvent) => {
    const { key, ctrlKey, metaKey, shiftKey } = e;
    const target = e.target as HTMLTextAreaElement;

    const value = target.value || "";
    const isMultiline = value.split("\n").length > 1;

    const ctrlMetaKey = ctrlKey || metaKey;
    const arrowUp = key === "ArrowUp";
    const arrowDown = key === "ArrowDown";
    const enter = key === "Enter";
    const isSlash = key === "/";

    // prev value from history
    if (arrowUp && ctrlMetaKey) {
      e.preventDefault();
      onArrowUp();
    }

    // next value from history
    if (arrowDown && ctrlMetaKey) {
      e.preventDefault();
      onArrowDown();
    }

    // execute query
    if (enter && !shiftKey && (!isMultiline || ctrlMetaKey)) {
      e.preventDefault();
      setAutocompleteDismissed(true);
      onEnter();
    }

    // comment code with #
    if (ctrlMetaKey && isSlash) {
      e.preventDefault();

      const { selectionStart, selectionEnd } = target;
      const {
        value: nextText,
        selectionStart: nextPosStart,
        selectionEnd: nextPosEnd
      } = toggleLineComment({ value, selectionStart, selectionEnd });

      onChange(nextText);
      setCaretPositionInput([nextPosStart, nextPosEnd]);
    }
  };

  useEffect(() => {
    if (autocompleteQuick) setAutocompleteDismissed(false);
  }, [autocompleteQuick]);

  return (
    <div
      className="vm-query-editor"
      ref={autocompleteAnchorEl}
    >
      <TextField
        value={value}
        label={`${label}${labelPostfix}`}
        type={"textarea"}
        error={error}
        onKeyDown={handleKeyDown}
        onChange={handleChange}
        onChangeCaret={handleChangeCaret}
        disabled={disabled}
        inputmode={"search"}
        caretPosition={caretPositionInput}
        onFocus={handleFocus}
        onBlur={onBlurred}
        endIcon={<QueryEditorHotkeysTip/>}
      />
      {AutocompleteEl && isShowAutocomplete && isFocused && !autocompleteDismissed && (
        <AutocompleteEl
          value={value}
          anchorEl={autocompleteAnchorEl}
          caretPosition={caretPositionAutocomplete}
          onSelect={handleSelect}
        />
      )}
    </div>
  );
};

export default QueryEditor;
