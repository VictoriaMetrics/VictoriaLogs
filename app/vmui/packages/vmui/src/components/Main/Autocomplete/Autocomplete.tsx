import { FC, useEffect, useRef, RefObject, ReactNode } from "preact/compat";
import classNames from "classnames";
import Popper from "../Popper/Popper";
import "./style.scss";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import AutocompleteDetailsPanel from "./AutocompleteDetailsPanel/AutocompleteDetailsPanel";
import AutocompleteList from "./AutocompleteList/AutocompleteList";
import { useAutocompleteOpen } from "./hooks/useAutocompleteOpen";
import { useAutocompleteKeyboard } from "./hooks/useAutocompleteKeyboard";
import { useAutocompleteOptions } from "./hooks/useAutocompleteOptions";
import AutocompleteKeyboardHints from "./AutocompleteKeyboardHints/AutocompleteKeyboardHints";
import LineLoader from "../LineLoader/LineLoader";
import { WarningIcon } from "../Icons";

export interface AutocompleteOptions {
  value: string;
  description?: string;
  type?: string;
  group?: string; // Section title used to group options in the list.
  meta?: string; // Optional secondary text shown on the right side of the option.
  icon?: ReactNode;
}

interface AutocompleteProps {
  value: string
  options: AutocompleteOptions[]
  anchor: RefObject<HTMLElement>
  minLength?: number
  fullWidth?: boolean
  noOptionsText?: string
  selected?: string[]
  label?: string
  disabledFullScreen?: boolean
  offset?: { top: number, left: number }
  maxDisplayResults?: { limit: number, message?: string }
  loading?: boolean;
  showKeyboardHints?: boolean;
  onSelect: (val: string, item: AutocompleteOptions) => void
  onOpenAutocomplete?: (val: boolean) => void
  onFoundOptions?: (val: AutocompleteOptions[]) => void
  onChangeWrapperRef?: (elementRef: RefObject<HTMLElement>) => void
}

export type AutocompleteFocusOption = {
  index: number,
  type?: FocusType
}

export enum FocusType {
  mouse,
  keyboard
}

const Autocomplete: FC<AutocompleteProps> = ({
  value,
  options,
  anchor,
  minLength = 2,
  fullWidth,
  selected,
  noOptionsText,
  label,
  disabledFullScreen,
  offset,
  maxDisplayResults,
  loading,
  showKeyboardHints = false,
  onSelect,
  onOpenAutocomplete,
  onFoundOptions,
  onChangeWrapperRef
}) => {
  const { isMobile } = useDeviceDetect();
  const wrapperRef = useRef<HTMLDivElement>(null);

  const { openAutocomplete, closeAutocomplete } = useAutocompleteOpen({
    value,
    minLength,
    onOpenAutocomplete
  });

  const { foundOptions, warningMessage } = useAutocompleteOptions({
    value,
    options,
    openAutocomplete,
    maxDisplayResults,
    onFoundOptions,
  });

  const { focusOption, setFocusOption } = useAutocompleteKeyboard({
    options: foundOptions,
    selected,
    onSelect,
    onClose: closeAutocomplete
  });

  const displayNoOptionsText = Boolean(noOptionsText && !foundOptions.length);

  useEffect(() => {
    onChangeWrapperRef?.(wrapperRef);
  }, [onChangeWrapperRef]);

  if (!loading && !displayNoOptionsText && !foundOptions.length) return null;

  return (
    <Popper
      open={openAutocomplete}
      buttonRef={anchor}
      placement="bottom-left"
      onClose={closeAutocomplete}
      fullWidth={fullWidth}
      title={isMobile ? label : undefined}
      disabledFullScreen={disabledFullScreen}
      offset={offset}
    >
      <div
        ref={wrapperRef}
        className={classNames({
          "vm-autocomplete": true,
          "vm-autocomplete_mobile": isMobile,
        })}
      >
        <div className="vm-autocomplete__content">
          <div className="vm-autocomplete-base-panel">
            {loading && <LineLoader/>}
            {displayNoOptionsText && <div className="vm-autocomplete__no-options">{noOptionsText}</div>}
            <AutocompleteList
              options={foundOptions}
              focusOption={focusOption}
              selectedOptions={selected}
              onSelect={onSelect}
              onFocus={setFocusOption}
              onClose={closeAutocomplete}
            />
            {warningMessage && (
              <div className="vm-autocomplete__warning"><WarningIcon/>{warningMessage}</div>
            )}
          </div>
          <AutocompleteDetailsPanel option={foundOptions[focusOption.index]}/>
        </div>
        {showKeyboardHints && !!foundOptions.length && <AutocompleteKeyboardHints/>}
      </div>
    </Popper>
  );
};

export default Autocomplete;
