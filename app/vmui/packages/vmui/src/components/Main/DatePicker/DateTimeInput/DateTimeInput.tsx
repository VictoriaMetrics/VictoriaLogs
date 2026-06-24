import { FC, useEffect, useRef, useState, RefObject, ChangeEvent, KeyboardEvent, useMemo } from "preact/compat";
import { CalendarIcon } from "../../Icons";
import DatePicker from "../DatePicker";
import Button from "../../Button/Button";
import { DATE_TIME_FORMAT } from "../../../../constants/date";
import InputMask from "react-input-mask";
import classNames from "classnames";
import "./style.scss";
import { vmDate } from "../../../../utils/time";

const formatStringDate = (val: string) => {
  const localDate = vmDate(val);
  return localDate.isValid() ? localDate.nano().format(DATE_TIME_FORMAT) : val;
};

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

  const [maskedValue, setMaskedValue] = useState(formatStringDate(value));
  const isValidDate = !!maskedValue && vmDate(maskedValue).isValid();
  const isoValue = useMemo(() => {
    return isValidDate ? vmDate.tz(maskedValue).nano().toISOString() : "";
  }, [maskedValue]);

  const datePickerValue = useMemo(() => isValidDate ? vmDate.tz(maskedValue) : vmDate().tz(), [maskedValue]);

  const [focusToTime, setFocusToTime] = useState(false);
  const [awaitChangeForEnter, setAwaitChangeForEnter] = useState(false);
  const error = isValidDate ? "" : "Invalid date format";

  const handleMaskedChange = (e: ChangeEvent<HTMLInputElement>) => {
    setMaskedValue(e.currentTarget.value);
  };

  const handleBlur = () => {
    if (!isValidDate) return;
    onChange(isoValue);
  };

  const handleKeyUp = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      if (!isValidDate) return;
      onChange(isoValue);
      setAwaitChangeForEnter(true);
    }
  };

  const handleChangeDate = (val: string) => {
    setMaskedValue(val);
    setFocusToTime(true);
  };

  useEffect(() => {
    const newValue = formatStringDate(value);
    if (newValue !== maskedValue) {
      setMaskedValue(newValue);
    }

    if (awaitChangeForEnter) {
      onEnter();
      setAwaitChangeForEnter(false);
    }
  }, [value]);

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
        value={maskedValue}
        autoCapitalize={"none"}
        inputMode={"numeric"}
        maskChar={null}
        onChange={handleMaskedChange}
        onBlur={handleBlur}
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
