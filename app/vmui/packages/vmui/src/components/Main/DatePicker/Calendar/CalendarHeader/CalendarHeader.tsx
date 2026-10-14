import { FC } from "preact/compat";
import { ArrowDownIcon, ArrowDropDownIcon } from "../../../Icons";
import type { VmDate } from "../../../../../utils/time";

interface CalendarHeaderProps {
  viewDate: VmDate
  onChangeViewDate: (date: VmDate) => void
  showArrowNav: boolean
  toggleDisplayYears: () => void
}

const HEADER_DATE_FORMAT = "MMMM YYYY";

const CalendarHeader: FC<CalendarHeaderProps> = ({ viewDate, showArrowNav, onChangeViewDate, toggleDisplayYears }) => {
  const handlePrevMonthClick = () => {
    onChangeViewDate(viewDate.subtract(1, "month"));
  };

  const handleNextMonthClick = () => {
    onChangeViewDate(viewDate.add(1, "month"));
  };

  return (
    <div className="vm-calendar-header">
      <div
        className="vm-calendar-header-left"
        onClick={toggleDisplayYears}
      >
        <span className="vm-calendar-header-left__date">
          {viewDate.format(HEADER_DATE_FORMAT)}
        </span>
        <div className="vm-calendar-header-left__select-year">
          <ArrowDropDownIcon/>
        </div>
      </div>
      {showArrowNav && (
        <div className="vm-calendar-header-right">
          <div
            className="vm-calendar-header-right__prev"
            onClick={handlePrevMonthClick}
          >
            <ArrowDownIcon/>
          </div>
          <div
            className="vm-calendar-header-right__next"
            onClick={handleNextMonthClick}
          >
            <ArrowDownIcon/>
          </div>
        </div>
      )}
    </div>
  );
};

export default CalendarHeader;
