import { useMemo, useCallback, useEffect, useRef } from "preact/compat";
import { useSearchParams } from "react-router-dom";
import {
  getDurationFromPeriod,
  getTimeParamsForDuration,
  isValidDate,
  normalizeTimeParams,
  relativeTimeOptions,
  timeParamsToDateRange,
  timePeriodToTimeParams,
} from "../../../utils/time";
import { NavigateOptions, RelativeTimeOption, TimeParams, TimePeriod } from "../../../types";

const TIME_QUERY_PARAMS = {
  RELATIVE: "relative_time",
  DURATION: "range_input",
  END_TIME: "end_input",
} as const;

type SetPeriodOptions =
  | { nextPeriod: TimePeriod }
  | { nextRelativeTime: RelativeTimeOption };

type TimeQueryParamValue = typeof TIME_QUERY_PARAMS[keyof typeof TIME_QUERY_PARAMS];

const getGroupKey = (key: TimeQueryParamValue, groupN: number = 0) => `g${groupN}.${key}`;

const defaultRelativeTime = relativeTimeOptions.find(d => d.isDefault) || relativeTimeOptions[0];

const NO_RELATIVE_TIME = "none";

const normalizeTimePeriod = (period: TimePeriod): TimePeriod => {
  const timeParams = timePeriodToTimeParams(period);
  const normalizedTimeParams = normalizeTimeParams(timeParams);

  return normalizedTimeParams === timeParams ? period : timeParamsToDateRange(normalizedTimeParams);
};

export const useTimePeriod = (groupN: number = 0) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const setSearchParamsRef = useRef(setSearchParams);

  const keys = useMemo(() => ({
    relative: getGroupKey(TIME_QUERY_PARAMS.RELATIVE, groupN),
    duration: getGroupKey(TIME_QUERY_PARAMS.DURATION, groupN),
    end: getGroupKey(TIME_QUERY_PARAMS.END_TIME, groupN),
  }), [groupN]);

  const durationStr = searchParams.get(keys.duration);
  const endTimeStr = searchParams.get(keys.end);
  const relativeTimeId = searchParams.get(keys.relative);

  const relativeTime = useMemo(() => {
    if (!relativeTimeId || relativeTimeId === NO_RELATIVE_TIME) return null;
    return relativeTimeOptions.find(d => d.id === relativeTimeId) || null;
  }, [relativeTimeId]);

  const getUrlParams = useCallback((payload: SetPeriodOptions) => {
    const params = new URLSearchParams();

    if ("nextPeriod" in payload) {
      const nextPeriod = normalizeTimePeriod(payload.nextPeriod);
      // for absolute time
      params.set(keys.end, nextPeriod.to);
      params.set(keys.duration, getDurationFromPeriod(nextPeriod));
      params.set(keys.relative, NO_RELATIVE_TIME);
    } else {
      // for relative time
      params.set(keys.end, payload.nextRelativeTime.until());
      params.set(keys.duration, payload.nextRelativeTime.duration);
      params.set(keys.relative, payload.nextRelativeTime.id);
    }

    return params;
  }, [keys]);

  const setPeriod = useCallback((payload: SetPeriodOptions, navigateOpts?: NavigateOptions) => {
    const timeParams = getUrlParams(payload);

    setSearchParamsRef.current(prev => {
      const nextParams = new URLSearchParams(prev);
      timeParams.forEach((value, key) => nextParams.set(key, value));
      return nextParams;
    }, navigateOpts);
  }, [getUrlParams]);

  const period: TimeParams = useMemo(() => {
    if (relativeTime) {
      return getTimeParamsForDuration(relativeTime.duration, relativeTime.until());
    }

    if (endTimeStr && isValidDate(endTimeStr)) {
      return getTimeParamsForDuration(durationStr || defaultRelativeTime.duration, endTimeStr);
    }

    return getTimeParamsForDuration(defaultRelativeTime.duration, defaultRelativeTime.until());
  }, [durationStr, endTimeStr, relativeTime]);

  const getCurrentPeriod = useCallback(() => {
    if (!relativeTime) return period;
    const { duration, until } = relativeTime;
    return getTimeParamsForDuration(duration, until());
  }, [relativeTime, period]);

  const refreshPeriod = useCallback(() => {
    if (!relativeTime && endTimeStr) return false;

    setPeriod(
      { nextRelativeTime: relativeTime || defaultRelativeTime },
      { replace: true }
    );

    return true;
  }, [relativeTime, setPeriod, endTimeStr]);

  useEffect(() => {
    setSearchParamsRef.current = setSearchParams;
  }, [setSearchParams]);

  return {
    period,
    setPeriod,
    refreshPeriod,
    getCurrentPeriod,
    relativeTime: relativeTime || (!endTimeStr ? defaultRelativeTime : null),
    getUrlParams,
  };
};
