import { FC, useEffect, useState, useMemo, useRef, useCallback } from "preact/compat";
import {
  getUTCByTimezone,
  nanosToIsoString,
  vmDate
} from "../../../../utils/time";
import TimeDurationSelector from "../TimeDurationSelector/TimeDurationSelector";
import { getAppModeEnable } from "../../../../utils/app-mode";
import { useTimeState } from "../../../../state/time/TimeStateContext";
import { ArrowDownIcon, ClockIcon } from "../../../Main/Icons";
import Button from "../../../Main/Button/Button";
import Popper from "../../../Main/Popper/Popper";
import Tooltip from "../../../Main/Tooltip/Tooltip";
import { DATE_TIME_FORMAT } from "../../../../constants/date";
import "./style.scss";
import useClickOutside from "../../../../hooks/useClickOutside";
import classNames from "classnames";
import { useAppState } from "../../../../state/common/StateContext";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import DateTimeInput from "../../../Main/DatePicker/DateTimeInput/DateTimeInput";
import useBoolean from "../../../../hooks/useBoolean";
import useWindowSize from "../../../../hooks/useWindowSize";
import { useQueryState } from "../../../../state/query/QueryStateContext";
import { useTimePeriod } from "../../../../pages/QueryPage/hooks/useTimePeriod";
import { RelativeTimeOption } from "../../../../types";
import useEventListener from "../../../../hooks/useEventListener";

type Props = {
  onOpenSettings?: () => void;
}

export const TimeSelector: FC<Props> = ({ onOpenSettings }) => {
  const { isMobile } = useDeviceDetect();
  const { isDarkTheme } = useAppState();
  const { queryHasTimeFilter } = useQueryState();

  const wrapperRef = useRef<HTMLDivElement>(null);
  const documentSize = useWindowSize();
  const displayFullDate = useMemo(() => documentSize.width > 1120, [documentSize]);

  const [until, setUntil] = useState<string>();
  const [from, setFrom] = useState<string>();

  const {
    period: { end, start },
    relativeTime,
    setPeriod,
  } = useTimePeriod();

  const { timezone } = useTimeState();
  const appModeEnable = getAppModeEnable();

  const {
    value: openOptions,
    toggle: toggleOpenOptions,
    setFalse: handleCloseOptions,
  } = useBoolean(false);

  const activeTimezone = useMemo(() => ({
    region: timezone,
    utc: getUTCByTimezone(timezone)
  }), [timezone]);

  useEffect(() => {
    handleSetUntil(end);
  }, [timezone, end]);

  useEffect(() => {
    handleSetFrom(start);
  }, [timezone, start]);

  const setDuration = (nextRelativeTime: RelativeTimeOption) => {
    setPeriod({ nextRelativeTime });
    handleCloseOptions();
  };

  const formatRange = useMemo(() => {
    const startFormat = vmDate(nanosToIsoString(start)).nano().format(DATE_TIME_FORMAT);
    const endFormat = vmDate(nanosToIsoString(end)).nano().format(DATE_TIME_FORMAT);
    return { start: startFormat, end: endFormat };
  }, [start, end, timezone]);

  const dateTitle = useMemo(() => {
    return relativeTime ? relativeTime.title : `${formatRange.start} - ${formatRange.end}`;
  }, [relativeTime, formatRange]);

  const fromPickerRef = useRef<HTMLDivElement>(null);
  const untilPickerRef = useRef<HTMLDivElement>(null);
  const buttonRef = useRef<HTMLDivElement>(null);

  const setTimeAndClosePicker = () => {
    if (from && until) {
      const nextPeriod = { from: from, to: until };
      setPeriod({ nextPeriod });
    }
    handleCloseOptions();
  };

  const handleSetFrom = (start: bigint) => {
    setFrom(nanosToIsoString(start));
  };

  const handleSetUntil = (end: bigint) => {
    setUntil(nanosToIsoString(end));
  };

  const handleOpenSettings = () => {
    onOpenSettings && onOpenSettings();
    handleCloseOptions();
  };

  const onCancelClick = useCallback(() => {
    handleSetUntil(end);
    handleSetFrom(start);
    handleCloseOptions();
  }, [end, start]);

  const handleKeyUp = useCallback((e: KeyboardEvent) => {
    if (!openOptions) return;
    if (e.key === "Escape") onCancelClick();
  }, [openOptions, onCancelClick]);


  useClickOutside(wrapperRef, (e) => {
    if (isMobile) return;
    const target = e.target as HTMLElement;
    const isButtonClick = buttonRef.current && buttonRef.current.contains(target);
    const isFromPicker = fromPickerRef?.current && fromPickerRef?.current?.contains(target);
    const isUntilPicker = untilPickerRef?.current && untilPickerRef?.current?.contains(target);
    if (isButtonClick || isFromPicker || isUntilPicker) return;
    handleCloseOptions();
  });

  useEventListener("keyup", handleKeyUp);

  return <>
    <div ref={buttonRef}>
      {isMobile ? (
        <div
          className="vm-mobile-option"
          onClick={toggleOpenOptions}
        >
          <span className="vm-mobile-option__icon"><ClockIcon/></span>
          <div className="vm-mobile-option-text">
            <span className="vm-mobile-option-text__label">Time range</span>
            <span className="vm-mobile-option-text__value">{dateTitle}</span>
          </div>
          <span className="vm-mobile-option__arrow"><ArrowDownIcon/></span>
        </div>
      ) : (
        <Tooltip title={displayFullDate ? "Time range controls" : dateTitle}>
          <Button
            className={appModeEnable ? "" : "vm-header-button"}
            variant="contained"
            color="primary"
            startIcon={<ClockIcon/>}
            onClick={toggleOpenOptions}
            aria-label="time range controls"
          >
            {displayFullDate && <span>{dateTitle}</span>}
          </Button>
        </Tooltip>
      )}
    </div>
    <Popper
      open={openOptions}
      buttonRef={buttonRef}
      placement="bottom-right"
      onClose={handleCloseOptions}
      clickOutside={false}
      title={isMobile ? "Time range controls" : ""}
    >
      <div
        className={classNames({
          "vm-time-selector": true,
          "vm-time-selector_mobile": isMobile
        })}
        ref={wrapperRef}
      >
        {queryHasTimeFilter && (
          <div className="vm-time-selector-warning">
            <p>Time range is overridden by the query `_time` filter.</p>
            <p>Remove `_time` from the query to use manual selection.</p>
            <p
              className="vm-time-selector-warning__interactive"
              onClick={handleOpenSettings}
            >
              To disable query time override in settings, click here.
            </p>
          </div>
        )}

        <div className="vm-time-selector-left">
          <div
            className={classNames({
              "vm-time-selector-left-inputs": true,
              "vm-time-selector-left-inputs_dark": isDarkTheme
            })}
          >
            <DateTimeInput
              value={from}
              label="From:"
              pickerLabel="Date From"
              pickerRef={fromPickerRef}
              onChange={setFrom}
              onEnter={setTimeAndClosePicker}
            />
            <DateTimeInput
              value={until}
              label="To:"
              pickerLabel="Date To"
              pickerRef={untilPickerRef}
              onChange={setUntil}
              onEnter={setTimeAndClosePicker}
            />
          </div>
          <div
            className="vm-time-selector-left-timezone"
            onClick={handleOpenSettings}
          >
            <div className="vm-time-selector-left-timezone__title">{activeTimezone.region}</div>
            <div className="vm-time-selector-left-timezone__utc">{activeTimezone.utc}</div>
          </div>
          <div className="vm-time-selector-left__controls">
            <Button
              color="error"
              variant="outlined"
              onClick={onCancelClick}
            >
              Cancel
            </Button>
            <Button
              color="primary"
              onClick={setTimeAndClosePicker}
            >
              Apply
            </Button>
          </div>
        </div>
        <TimeDurationSelector
          relativeTime={relativeTime}
          setDuration={setDuration}
        />
      </div>
    </Popper>
  </>;
};
