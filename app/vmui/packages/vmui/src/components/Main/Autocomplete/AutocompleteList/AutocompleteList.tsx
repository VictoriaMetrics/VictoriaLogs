import { FC, useEffect, useRef } from "preact/compat";
import { AutocompleteFocusOption, AutocompleteOptions, FocusType } from "../Autocomplete";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import classNames from "classnames";
import { DoneIcon } from "../../Icons";
import "./style.scss";

type Props = {
  options: AutocompleteOptions[];
  focusOption: AutocompleteFocusOption;
  selectedOptions?: string[];
  hideOptions: boolean;
  disabled?: boolean;
  onSelect: (val: string, item: AutocompleteOptions) => void
  onFocus: (val: AutocompleteFocusOption) => void;
  onClose: () => void;
}

const AutocompleteList: FC<Props> = ({
  options,
  focusOption,
  selectedOptions,
  hideOptions,
  disabled,
  onSelect,
  onFocus,
  onClose,
}) => {
  const { isMobile } = useDeviceDetect();
  const listRef = useRef<HTMLDivElement>(null);

  const isSelected = (val: string) => selectedOptions?.includes(val);

  const createHandlerSelect = (item: AutocompleteOptions) => () => {
    if (disabled) return;
    onSelect(item.value, item);
    if (!selectedOptions) onClose();
  };

  const createHandlerMouseEnter = (index: number) => () => {
    onFocus({ index, type: FocusType.mouse });
  };

  const scrollToValue = () => {
    if (!listRef.current || focusOption.type === FocusType.mouse) return;
    const target = listRef.current.childNodes[focusOption.index] as HTMLElement;
    if (target?.scrollIntoView) target.scrollIntoView({ block: "center" });
  };

  useEffect(scrollToValue, [focusOption, options]);

  if (hideOptions) return null;

  return (
    <div
      className={classNames({
        "vm-autocomplete-list": true,
        "vm-autocomplete-list_mobile": isMobile,
      })}
      ref={listRef}
    >
      {options.map((option, i) =>
        <div
          className={classNames({
            "vm-list-item": true,
            "vm-list-item_mobile": isMobile,
            "vm-list-item_active": i === focusOption.index,
            "vm-list-item_multiselect": selectedOptions,
            "vm-list-item_multiselect_selected": isSelected(option.value),
            "vm-list-item_with-icon": option.icon,
          })}
          id={`$autocomplete$${option.value}`}
          key={`${i}${option.value}`}
          onClick={createHandlerSelect(option)}
          onMouseEnter={createHandlerMouseEnter(i)}
        >
          {isSelected(option.value) && <DoneIcon/>}
          {option.icon}
          <span>{option.value}</span>
        </div>
      )}
    </div>
  );
};

export default AutocompleteList;
