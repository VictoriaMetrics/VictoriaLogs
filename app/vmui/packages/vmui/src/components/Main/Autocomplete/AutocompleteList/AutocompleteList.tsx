import { FC, Fragment, useEffect, useRef } from "preact/compat";
import { AutocompleteFocusOption, AutocompleteOptions, FocusType } from "../Autocomplete";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import classNames from "classnames";
import { DoneIcon } from "../../Icons";
import "./style.scss";

type Props = {
  options: AutocompleteOptions[];
  focusOption: AutocompleteFocusOption;
  selectedOptions?: string[];
  onSelect: (val: string, item: AutocompleteOptions) => void
  onFocus: (val: AutocompleteFocusOption) => void;
  onClose: () => void;
}

const AutocompleteList: FC<Props> = ({
  options,
  focusOption,
  selectedOptions,
  onSelect,
  onFocus,
  onClose,
}) => {
  const { isMobile } = useDeviceDetect();
  const listRef = useRef<HTMLDivElement>(null);
  const listIdRef = useRef(`vm-autocomplete-list-${Math.random().toString(36).slice(2)}`);

  const isSelected = (val: string) => selectedOptions?.includes(val);

  const getOptionId = (index: number) => `${listIdRef.current}-option-${index}`;

  const createHandlerSelect = (item: AutocompleteOptions) => () => {
    onSelect(item.value, item);
    if (!selectedOptions) onClose();
  };

  const createHandlerMouseMove = (index: number) => () => {
    if (focusOption.index === index && focusOption.type === FocusType.mouse) return;
    onFocus({ index, type: FocusType.mouse });
  };

  const scrollToValue = () => {
    if (!listRef.current || focusOption.type === FocusType.mouse) return;
    const target = document.getElementById(getOptionId(focusOption.index));
    if (target?.scrollIntoView) target.scrollIntoView({ block: "center" });
  };

  useEffect(scrollToValue, [focusOption, options]);

  if (!options.length) return null;

  return (
    <div
      className={classNames({
        "vm-list": true,
        "vm-autocomplete-list": true,
        "vm-autocomplete-list_mobile": isMobile,
      })}
      ref={listRef}
      onMouseDown={(e) => e.preventDefault()}
    >
      {options.map((option, i) => {
        const title = options[i - 1]?.group !== option.group ? option.group : null;

          return (
            <Fragment key={`${i}${option.value}`}>
              {title && <div className="vm-autocomplete__title">{title}</div>}
              <div
                className={classNames({
                  "vm-autocomplete-list-item": true,
                  "vm-list-item": true,
                  "vm-list-item_mobile": isMobile,
                  "vm-list-item_active": i === focusOption.index,
                  "vm-list-item_multiselect": selectedOptions,
                  "vm-list-item_multiselect_selected": isSelected(option.value),
                  "vm-list-item_with-icon": option.icon,
                })}
                id={getOptionId(i)}
                onClick={createHandlerSelect(option)}
                onMouseMove={createHandlerMouseMove(i)}
              >
                {isSelected(option.value) && <DoneIcon/>}
                {option.icon && <span className="vm-list-item__icon">{option.icon}</span>}
                <span className="vm-list-item__title vm-autocomplete-list-item__title">{option.value}</span>
                {option.meta && (<span className="vm-list-item__meta">{option.meta}</span>)}
              </div>
            </Fragment>
          );
        }
      )}
    </div>
  );
};

export default AutocompleteList;
