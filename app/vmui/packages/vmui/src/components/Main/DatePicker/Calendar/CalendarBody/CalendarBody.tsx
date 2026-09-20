import { FC, useMemo } from "preact/compat";
import classNames from "classnames";
import Tooltip from "../../../Tooltip/Tooltip";
import { DATE_FORMAT } from "../../../../../constants/date";
import { getNowInTimezone, type VmDate } from "../../../../../utils/time";

interface CalendarBodyProps {
  viewDate: VmDate
  selectDate: VmDate
  onChangeSelectDate: (date: VmDate) => void
}

const DAY_FORMAT = "D";
const WEEKDAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
const CALENDAR_CELLS_COUNT = 42;

const CalendarBody: FC<CalendarBodyProps> = ({ viewDate, selectDate, onChangeSelectDate }) => {
  const todayKey = getNowInTimezone().format(DATE_FORMAT);
  const selectedDateKey = useMemo(() => selectDate.format(DATE_FORMAT), [selectDate]);

  const days: (VmDate | null)[] = useMemo(() => {
    const result = Array<VmDate | null>(CALENDAR_CELLS_COUNT).fill(null);
    const startDate = viewDate.startOf("month");
    const endDate = viewDate.endOf("month");
    const daysCount = endDate.diff(startDate, "day") + 1;
    const monthDays = Array.from({ length: daysCount }, (_, i) => startDate.add(i, "day"));
    const startOfWeek = startDate.day();

    result.splice(startOfWeek, daysCount, ...monthDays);

    return result;
  }, [viewDate]);

  const createHandlerSelectDate = (date: VmDate | null) => () => {
    if (!date) return;
    onChangeSelectDate(date);
  };

  return (
    <div className="vm-calendar-body">
      {WEEKDAYS.map(weekday => (
        <Tooltip
          title={weekday}
          key={weekday}
        >
          <div className="vm-calendar-body-cell vm-calendar-body-cell_weekday">
            {weekday[0]}
          </div>
        </Tooltip>
      ))}

      {days.map((date, i) => {
        const dateKey = date?.format(DATE_FORMAT);

        return (
          <div
            className={classNames({
              "vm-calendar-body-cell": true,
              "vm-calendar-body-cell_day": true,
              "vm-calendar-body-cell_day_empty": !date,
              "vm-calendar-body-cell_day_active": dateKey === selectedDateKey,
              "vm-calendar-body-cell_day_today": dateKey === todayKey
            })}
            key={dateKey ?? i}
            onClick={createHandlerSelectDate(date)}
          >
            {date?.format(DAY_FORMAT)}
          </div>
        );
      })}
    </div>
  );
};

export default CalendarBody;
