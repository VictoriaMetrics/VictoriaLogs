import { FC } from "preact/compat";
import "./style.scss";

/**
 * Renders keyboard hints for shortcuts handled by {@link useAutocompleteKeyboard}.
 *
 * @see useAutocompleteKeyboard
 */
const AutocompleteKeyboardHints: FC = () => {

  return (
    <div className="vm-autocomplete-keyboard-hints">
      <p className="vm-autocomplete-keyboard-hints__item">
        <kbd>↑</kbd>/<kbd>↓</kbd> <span>navigate</span>
      </p>

      <p className="vm-autocomplete-keyboard-hints__item">
        <kbd>Shift</kbd> + <kbd>↑</kbd>/<kbd>↓</kbd> <span>jump sections</span>
      </p>

      <p className="vm-autocomplete-keyboard-hints__item">
        <kbd>Enter</kbd> <span>select</span>
      </p>

      <p className="vm-autocomplete-keyboard-hints__item">
        <kbd>Esc</kbd> <span>close</span>
      </p>
    </div>
  );
};

export default AutocompleteKeyboardHints;
