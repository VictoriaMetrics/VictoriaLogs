import { FC, useEffect, useMemo } from "preact/compat";
import classNames from "classnames";
import { getNowInTimezone, type VmDate } from "../../../../../utils/time";

interface CalendarMonthsProps {
  viewDate: VmDate,
  selectDate: VmDate
  onChangeViewDate: (date: VmDate) => void
}

const MONTH_LABEL_FORMAT = "MMMM";
const MONTH_KEY_FORMAT = "YYYY-MM";
const MONTHS_COUNT = 12;

const MonthsList: FC<CalendarMonthsProps> = ({ viewDate, selectDate, onChangeViewDate }) => {
  const now = getNowInTimezone();
  const todayMonthKey = now.format(MONTH_KEY_FORMAT);
  const selectedMonthKey = useMemo(() => selectDate.format(MONTH_KEY_FORMAT), [selectDate]);

  const months: VmDate[] = useMemo(() => {
    return Array.from({ length: MONTHS_COUNT }, (_, i) => viewDate.month(i));
  }, [viewDate]);

  useEffect(() => {
    const selectedEl = document.getElementById(`vm-calendar-year-${selectedMonthKey}`);
    if (!selectedEl) return;
    selectedEl.scrollIntoView({ block: "center" });
  }, [selectedMonthKey]);

  const createHandlerClick = (date: VmDate) => () => {
    onChangeViewDate(date);
  };

  return (
    <div className="vm-calendar-years">
      {months.map(m => {
        const monthKey = m.format(MONTH_KEY_FORMAT);
        const monthLabel = m.format(MONTH_LABEL_FORMAT);

        return (
          <div
            className={classNames({
              "vm-calendar-years__year": true,
              "vm-calendar-years__year_selected": monthKey === selectedMonthKey,
              "vm-calendar-years__year_today": monthKey === todayMonthKey
            })}
            id={`vm-calendar-year-${monthKey}`}
            key={monthKey}
            onClick={createHandlerClick(m)}
          >
            {monthLabel}
          </div>
        );
      })}
    </div>
  );
};

export default MonthsList;
