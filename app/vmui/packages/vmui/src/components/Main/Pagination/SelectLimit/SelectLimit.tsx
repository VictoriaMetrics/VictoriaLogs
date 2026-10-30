import { useMemo, useRef } from "preact/compat";
import { ArrowDropDownIcon, SpinnerIcon, WarningIcon } from "../../Icons";
import useBoolean from "../../../../hooks/useBoolean";
import Popper from "../../Popper/Popper";
import classNames from "classnames";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import "./style.scss";
import Button from "../../Button/Button";
import TextField from "../../TextField/TextField";
import { useState } from "react";
import { ComponentChildren } from "preact";

interface SelectLimitProps<T extends string | number> {
  limit: T | T[];
  label?: string;
  options?: T[];
  allowUnlimited?: boolean;
  emptyValueLabel?: string;
  emptyValue?: T;
  isLoading?: boolean;
  error?: string;
  searchable?: boolean;
  textNoOptions?: string;
  onChange: (val: T) => void;
  onOpenSelect?: () => void;
  renderOptionLabel?: (value: T, isLabel: boolean) => ComponentChildren;
}

const defaultLimits = [10, 25, 50, 100, 250, 500, 1000];

export const SelectLimit = <T extends string | number>(props: SelectLimitProps<T>) => {
  const {
    limit,
    label,
    options,
    allowUnlimited,
    emptyValueLabel,
    emptyValue,
    isLoading,
    error,
    searchable,
    textNoOptions,
    onChange,
    onOpenSelect,
    renderOptionLabel
  } = props;

  const [search, setSearch] = useState("");

  const { isMobile } = useDeviceDetect();
  const buttonRef = useRef<HTMLDivElement>(null);

  const isMultiple = Array.isArray(limit);
  const selectedLimits = useMemo(() => (Array.isArray(limit) ? limit : [limit]), [limit]);

  const limits = useMemo(() => {
    const arr = options || defaultLimits;
    return allowUnlimited ? [...arr, 0] : arr;
  }, [allowUnlimited, options]);

  const filteredLimits = useMemo(() => {
    if (!searchable || !search.trim()) return limits;
    const q = search.toLowerCase();
    return limits.filter(v => String(v).toLowerCase().includes(q));
  }, [limits, search, searchable]);

  const {
    value: openList,
    toggle: toggleOpenList,
    setFalse: handleClose,
  } = useBoolean(false);

  const handleClickSelect = () => {
    toggleOpenList();
    if (!openList) onOpenSelect?.();
  };

  const handleChangeLimit = (n: T) => () => {
    onChange(n);
    if (!isMultiple) handleClose();
  };

  const defaultLabel = isMobile ? "Rows" : "Rows per page";
  const displayLabel = label ?? defaultLabel;

  const defaultValue = (Array.isArray(limit) ? limit.join(", ") : limit) || emptyValueLabel || "All";
  const displayValue = renderOptionLabel && !Array.isArray(limit) ? renderOptionLabel(limit, true) : defaultValue;

  return (
    <>
      <div
        className="vm-select-limits-button"
        onClick={handleClickSelect}
        ref={buttonRef}
      >
        <div
          className="vm-select-limits-button__text"
          title={isMultiple ? selectedLimits.join(", ") : undefined}
        >
          {displayLabel && `${displayLabel}: `}<b>{displayValue}</b>
        </div>
        <ArrowDropDownIcon/>
      </div>
      <Popper
        open={openList}
        onClose={handleClose}
        placement="bottom-right"
        buttonRef={buttonRef}
      >
        <div className="vm-select-limits">
          {isLoading && (
            <Button
              disabled
              color="gray"
              variant="text"
              startIcon={<SpinnerIcon/>}
            >
              loading...
            </Button>
          )}

          {error && (
            <div className="vm-select-limits__error">
              <WarningIcon/>
              {error}
            </div>
          )}

          {!isLoading && !error && limits.length === 0 && (
            <div className="vm-select-limits__empty">
              {textNoOptions || "No options available"}
            </div>
          )}

          {searchable && !isLoading && !error && !!limits.length && (
            <div className="vm-select-limits__search">
              <TextField
                autofocus
                label="Search"
                value={search}
                onChange={setSearch}
              />
            </div>
          )}

          <div className="vm-list vm-select-limits-list">
            {!isLoading && !error && filteredLimits.map(n => (
              <div
                className={classNames({
                  "vm-list-item": true,
                  "vm-select-limits__item": true,
                  "vm-list-item_mobile": isMobile,
                  "vm-list-item_active": isMultiple && !selectedLimits.length
                    ? n === emptyValue
                    : selectedLimits.includes(n as T),
                })}
                key={n}
                onClick={handleChangeLimit(n as T)}
              >
                {renderOptionLabel ? renderOptionLabel(n as T, false) : n || "All"}
              </div>
            ))}
          </div>
        </div>
      </Popper>
    </>
  );
};

export default SelectLimit;
