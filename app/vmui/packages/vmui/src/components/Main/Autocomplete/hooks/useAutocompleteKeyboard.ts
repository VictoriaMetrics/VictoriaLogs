import { useCallback, useEffect, useRef, useState } from "preact/compat";
import useEventListener from "../../../../hooks/useEventListener";
import { AutocompleteOptions, FocusType, AutocompleteFocusOption } from "../Autocomplete";

type UseAutocompleteKeyboardArgs = {
  options: AutocompleteOptions[];
  selected?: string[];
  onSelect: (val: string, item: AutocompleteOptions) => void;
  onClose: () => void;
};

export const useAutocompleteKeyboard = ({
  options,
  selected,
  onSelect,
  onClose,
}: UseAutocompleteKeyboardArgs) => {
  const [focusOption, setFocusOption] = useState<AutocompleteFocusOption>({ index: -1 });

  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    const { key, ctrlKey, metaKey, shiftKey, altKey } = e;
    const modifiers = ctrlKey || metaKey || shiftKey || altKey;
    const hasOptions = options.length > 0;

    const hasOnlyShift = shiftKey && !ctrlKey && !metaKey && !altKey;

    if (key === "ArrowUp" && !modifiers && hasOptions) {
      e.preventDefault();
      setFocusOption(({ index }) => ({
        index: index <= 0 ? 0 : index - 1,
        type: FocusType.keyboard,
      }));
    }

    if (key === "ArrowDown" && !modifiers && hasOptions) {
      e.preventDefault();
      const lastIndex = options.length - 1;
      setFocusOption(({ index }) => ({
        index: index >= lastIndex ? lastIndex : index + 1,
        type: FocusType.keyboard,
      }));
    }

    if (key === "ArrowUp" && hasOnlyShift && hasOptions) {
      e.preventDefault();
      const currentGroup = options[focusOption.index]?.group;
      const nextIndex = options.findIndex((item, i) => {
        return i < focusOption.index && item.group !== currentGroup;
      });

      if (nextIndex === -1) return;

      setFocusOption({
        index: nextIndex,
        type: FocusType.keyboard,
      });
    }


    if (key === "ArrowDown" && hasOnlyShift && hasOptions) {
      e.preventDefault();
      const currentGroup = options[focusOption.index]?.group;
      const nextIndex = options.findIndex((item, i) => {
        return i > focusOption.index && item.group !== currentGroup;
      });

      if (nextIndex === -1) return;

      setFocusOption({
        index: nextIndex,
        type: FocusType.keyboard,
      });
    }

    if (key === "Enter") {
      const item = options[focusOption.index];
      if (!item) return;

      e.preventDefault();
      e.stopPropagation();

      onSelect(item.value, item);
      if (!selected) onClose();
    }

    if (key === "Escape") {
      onClose();
    }
  }, [options, focusOption.index, onSelect, selected, onClose]);

  const documentRef = useRef(document.body);
  useEventListener("keydown", handleKeyDown, documentRef, { capture: true });

  useEffect(() => {
    setFocusOption({ index: -1 });
  }, [options]);

  return {
    focusOption,
    setFocusOption,
  };
};
