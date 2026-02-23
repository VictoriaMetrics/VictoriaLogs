import { FC, useCallback, useEffect, useMemo, useRef, useState, JSX, RefObject } from "preact/compat";
import classNames from "classnames";
import Popper from "../Popper/Popper";
import "./style.scss";
import { RefreshIcon } from "../Icons";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import useBoolean from "../../../hooks/useBoolean";
import useEventListener from "../../../hooks/useEventListener";
import AutocompleteDetailsPanel from "./AutocompleteDetailsPanel/AutocompleteDetailsPanel";
import AutocompleteList from "./AutocompleteList/AutocompleteList";

export interface AutocompleteOptions {
  value: string;
  description?: string;
  type?: string;
  icon?: JSX.Element
}

interface AutocompleteProps {
  value: string
  options: AutocompleteOptions[]
  anchor: RefObject<HTMLElement>
  disabled?: boolean
  minLength?: number
  fullWidth?: boolean
  noOptionsText?: string
  selected?: string[]
  label?: string
  disabledFullScreen?: boolean
  offset?: { top: number, left: number }
  maxDisplayResults?: { limit: number, message?: string }
  loading?: boolean;
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
  disabled,
  minLength = 2,
  fullWidth,
  selected,
  noOptionsText,
  label,
  disabledFullScreen,
  offset,
  maxDisplayResults,
  loading,
  onSelect,
  onOpenAutocomplete,
  onFoundOptions,
  onChangeWrapperRef
}) => {
  const { isMobile } = useDeviceDetect();
  const wrapperRef = useRef<HTMLDivElement>(null);

  const [focusOption, setFocusOption] = useState<AutocompleteFocusOption>({ index: -1 });
  const [showMessage, setShowMessage] = useState("");
  const [totalFound, setTotalFound] = useState(0);
  const warningMessage = `Shown ${maxDisplayResults?.limit} results out of ${totalFound}. ${showMessage}`;

  const {
    value: openAutocomplete,
    setValue: setOpenAutocomplete,
    setFalse: handleCloseAutocomplete,
  } = useBoolean(false);

  const foundOptions = useMemo(() => {
    if (!openAutocomplete) return [];
    try {
      const regexp = new RegExp(String(value.trim()), "i");
      const found = options.filter((item) => regexp.test(item.value));
      const sorted = found.sort((a, b) => {
        if (a.value.toLowerCase() === value.trim().toLowerCase()) return -1;
        if (b.value.toLowerCase() === value.trim().toLowerCase()) return 1;
        return (a.value.match(regexp)?.index || 0) - (b.value.match(regexp)?.index || 0);
      });
      setTotalFound(sorted.length);
      setShowMessage(sorted.length > Number(maxDisplayResults?.limit) ? maxDisplayResults?.message || "" : "");
      return maxDisplayResults?.limit ? sorted.slice(0, maxDisplayResults.limit) : sorted;
    } catch (e) {
      return [];
    }
  }, [openAutocomplete, options, value]);

  const hideFoundedOptions = useMemo(() => {
    return foundOptions.length === 1 && foundOptions[0]?.value === value;
  }, [foundOptions]);

  const displayNoOptionsText = useMemo(() => {
    return noOptionsText && !foundOptions.length;
  }, [noOptionsText, foundOptions]);

  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    const { key, ctrlKey, metaKey, shiftKey } = e;
    const modifiers = ctrlKey || metaKey || shiftKey;
    const hasOptions = foundOptions.length && !hideFoundedOptions;

    if (key === "ArrowUp" && !modifiers && hasOptions) {
      e.preventDefault();
      setFocusOption(({ index }) => ({
        index: index <= 0 ? 0 : index - 1,
        type: FocusType.keyboard
      }));
    }

    if (key === "ArrowDown" && !modifiers && hasOptions) {
      e.preventDefault();
      const lastIndex = foundOptions.length - 1;
      setFocusOption(({ index }) => ({
        index: index >= lastIndex ? lastIndex : index + 1,
        type: FocusType.keyboard
      }));
    }

    if (key === "Enter") {
      const item = foundOptions[focusOption.index];
      item && onSelect(item.value, item);
      if (!selected) handleCloseAutocomplete();
    }

    if (key === "Escape") {
      handleCloseAutocomplete();
    }
  }, [focusOption, foundOptions, hideFoundedOptions, handleCloseAutocomplete, onSelect, selected]);

  useEffect(() => {
    setOpenAutocomplete(value.length >= minLength);
  }, [value, options]);

  useEventListener("keydown", handleKeyDown);

  useEffect(() => {
    setFocusOption({ index: -1 });
  }, [foundOptions]);

  useEffect(() => {
    onOpenAutocomplete && onOpenAutocomplete(openAutocomplete);
  }, [openAutocomplete]);

  useEffect(() => {
    onFoundOptions && onFoundOptions(hideFoundedOptions ? [] : foundOptions);
  }, [foundOptions, hideFoundedOptions]);

  useEffect(() => {
    onChangeWrapperRef && onChangeWrapperRef(wrapperRef);
  }, [wrapperRef]);

  return (
    <Popper
      open={openAutocomplete}
      buttonRef={anchor}
      placement="bottom-left"
      onClose={handleCloseAutocomplete}
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
        <div className="vm-autocomplete-base-panel">
          {loading && <div className="vm-autocomplete__loader"><RefreshIcon/><span>Loading...</span></div>}
          {displayNoOptionsText && <div className="vm-autocomplete__no-options">{noOptionsText}</div>}
          <AutocompleteList
            options={foundOptions}
            focusOption={focusOption}
            selectedOptions={selected}
            hideOptions={hideFoundedOptions}
            disabled={disabled}
            onSelect={onSelect}
            onFocus={setFocusOption}
            onClose={handleCloseAutocomplete}
          />
          {showMessage && <div className="vm-autocomplete__warning">{warningMessage}</div>}
        </div>
        <AutocompleteDetailsPanel option={foundOptions[focusOption.index]}/>
      </div>
    </Popper>
  );
};

export default Autocomplete;
