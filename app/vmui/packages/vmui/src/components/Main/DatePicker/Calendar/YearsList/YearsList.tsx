import { FC, useEffect, useMemo } from "preact/compat";
import classNames from "classnames";
import { getNowInTimezone, type VmDate } from "../../../../../utils/time";

interface CalendarYearsProps {
  viewDate: VmDate
  onChangeViewDate: (date: VmDate) => void
}

const YEAR_FORMAT = "YYYY";
const DISPLAY_YEARS_COUNT = 18;

const YearsList: FC<CalendarYearsProps> = ({ viewDate, onChangeViewDate }) => {
  const today = getNowInTimezone().format(YEAR_FORMAT);
  const selectedYear = useMemo(() => viewDate.format(YEAR_FORMAT), [viewDate]);

  const years: VmDate[] = useMemo(() => {
    const now = getNowInTimezone();
    const startYear = now.subtract(DISPLAY_YEARS_COUNT / 2, "year");
    return Array.from({ length: DISPLAY_YEARS_COUNT }, (_, i) => startYear.add(i, "year"));
  }, []);

  useEffect(() => {
    const selectedEl = document.getElementById(`vm-calendar-year-${selectedYear}`);
    if (!selectedEl) return;
    selectedEl.scrollIntoView({ block: "center" });
  }, [selectedYear]);

  const createHandlerClick = (year: VmDate) => () => {
    onChangeViewDate(year);
  };

  return (
    <div className="vm-calendar-years">
      {years.map(y => {
        const formattedYear = y.format(YEAR_FORMAT);

        return (
          <div
            className={classNames({
              "vm-calendar-years__year": true,
              "vm-calendar-years__year_selected": formattedYear === selectedYear,
              "vm-calendar-years__year_today": formattedYear === today
            })}
            id={`vm-calendar-year-${formattedYear}`}
            key={formattedYear}
            onClick={createHandlerClick(y)}
          >
            {formattedYear}
          </div>
        );
      })}
    </div>
  );
};

export default YearsList;
