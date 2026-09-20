import { forwardRef, RefObject } from "preact/compat";
import Calendar from "../../Main/DatePicker/Calendar/Calendar";
import Popper from "../../Main/Popper/Popper";
import { DATE_TIME_FORMAT } from "../../../constants/date";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import useBoolean from "../../../hooks/useBoolean";
import useEventListener from "../../../hooks/useEventListener";
import type { VmDate } from "../../../utils/time";

interface DatePickerProps {
  date: VmDate,
  targetRef: RefObject<HTMLElement>;
  format?: string
  label?: string
  onChange: (val: string) => void
}

const DatePicker = forwardRef<HTMLDivElement, DatePickerProps>(({
  date,
  targetRef,
  format = DATE_TIME_FORMAT,
  onChange,
  label
}, ref) => {
  const { isMobile } = useDeviceDetect();

  const {
    value: openCalendar,
    toggle: toggleOpenCalendar,
    setFalse: handleCloseCalendar,
  } = useBoolean(false);

  const handleChangeDate = (val: string) => {
    onChange(val);
    handleCloseCalendar();
  };

  const handleKeyUp = (e: KeyboardEvent) => {
    if (e.key === "Escape" || e.key === "Enter") handleCloseCalendar();
  };

  useEventListener("click", toggleOpenCalendar, targetRef);
  useEventListener("keyup", handleKeyUp);

  return (<>
    <Popper
      open={openCalendar}
      buttonRef={targetRef}
      placement="bottom-right"
      onClose={handleCloseCalendar}
      title={isMobile ? label : undefined}
    >
      <div ref={ref}>
        <Calendar
          date={date}
          format={format}
          onChange={handleChangeDate}
        />
      </div>
    </Popper>
  </>);
});

export default DatePicker;
