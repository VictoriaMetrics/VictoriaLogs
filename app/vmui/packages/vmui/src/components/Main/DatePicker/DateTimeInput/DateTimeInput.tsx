import { FC, useEffect, useRef, useState, RefObject, useMemo } from "preact/compat";
import { CalendarIcon } from "../../Icons";
import DatePicker from "../DatePicker";
import Button from "../../Button/Button";
import InputMask from "react-input-mask";
import classNames from "classnames";
import "./style.scss";
import { vmDate } from "../../../../utils/time";
import { parseDateTimeInputValue } from "./utils";
import { TargetedEvent } from "preact";

interface DateTimeInputProps {
  value?:  string;
  label: string;
  pickerLabel: string;
  pickerRef: RefObject<HTMLDivElement>;
  onChange: (date: string) => void;
  onEnter: () => void;
}

const DateTimeInput: FC<DateTimeInputProps> = ({
  value = "",
  label,
  pickerLabel,
  pickerRef,
  onChange,
  onEnter
}) => {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const [inputRef, setInputRef] = useState<HTMLInputElement | null>(null);

  const isValidDate = useMemo(() => !!parseDateTimeInputValue(value), [value]);
  const datePickerValue = useMemo(() => isValidDate ? vmDate.tz(value) : vmDate().tz(), [value]);

  const [focusToTime, setFocusToTime] = useState(false);
  const error = isValidDate ? "" : "Invalid date format";

  const handleMaskedChange = (e: TargetedEvent<HTMLInputElement, Event>) => {
    onChange(e.currentTarget.value);
  };

  const handleKeyUp = (e: KeyboardEvent) => {
    if (e.key === "Enter" && isValidDate) onEnter();
  };

  const handleChangeDate = (val: string) => {
    onChange(val);
    setFocusToTime(true);
  };

  useEffect(() => {
    if (focusToTime && inputRef) {
      inputRef.focus();
      inputRef.setSelectionRange(11, 11);
      setFocusToTime(false);
    }
  }, [focusToTime]);

  return (
    <div
      className={classNames({
        "vm-date-time-input": true,
        "vm-date-time-input_error": error
      })}
    >
      <label>{label}</label>
      <InputMask
        tabIndex={1}
        inputRef={setInputRef}
        mask="9999-99-99 99:99:99.999999999"
        placeholder="YYYY-MM-DD HH:mm:ss.SSSSSSSSS"
        value={value}
        autoCapitalize={"none"}
        inputMode={"numeric"}
        maskChar={null}
        onChange={handleMaskedChange}
        onKeyUp={handleKeyUp}
      />
      {error && (
        <span className="vm-date-time-input__error-text">{error}</span>
      )}
      <div
        className="vm-date-time-input__icon"
        ref={wrapperRef}
      >
        <Button
          variant="text"
          color="gray"
          size="small"
          startIcon={<CalendarIcon/>}
          aria-label="calendar"
        />
      </div>
      <DatePicker
        label={pickerLabel}
        ref={pickerRef}
        date={datePickerValue}
        onChange={handleChangeDate}
        targetRef={wrapperRef}
      />
    </div>
  );
};

export default DateTimeInput;
