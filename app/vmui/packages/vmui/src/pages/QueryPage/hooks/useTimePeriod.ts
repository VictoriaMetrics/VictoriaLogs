import { useMemo, useCallback } from "preact/compat";
import { useSearchParams } from "react-router-dom";
import {
  getDurationFromPeriod,
  getTimeperiodForDuration,
  relativeTimeOptions
} from "../../../utils/time";
import { NavigateOptions, RelativeTimeOption, TimeParams, TimePeriod } from "../../../types";
import dayjs from "dayjs";

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

export const useTimePeriod = (groupN: number = 0) => {
  const [searchParams, setSearchParams] = useSearchParams();

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
      // for absolute time
      params.set(keys.end, payload.nextPeriod.to.toISOString());
      params.set(keys.duration, getDurationFromPeriod(payload.nextPeriod));
      params.set(keys.relative, NO_RELATIVE_TIME);
    } else {
      // for relative time
      params.set(keys.end, payload.nextRelativeTime.until().toISOString());
      params.set(keys.duration, payload.nextRelativeTime.duration);
      params.set(keys.relative, payload.nextRelativeTime.id);
    }

    return params;
  }, [keys]);

  const setPeriod = useCallback((payload: SetPeriodOptions, navigateOpts?: NavigateOptions) => {
    const timeParams = getUrlParams(payload);

    setSearchParams(prev => {
      const nextParams = new URLSearchParams(prev);
      timeParams.forEach((value, key) => nextParams.set(key, value));
      return nextParams;
    }, navigateOpts);
  }, [setSearchParams, getUrlParams]);

  const period: TimeParams = useMemo(() => {
    if (relativeTime) {
      return getTimeperiodForDuration(relativeTime.duration, relativeTime.until());
    }

    if (endTimeStr) {
      const parsedEnd = dayjs(endTimeStr);

      if (parsedEnd.isValid()) {
        return getTimeperiodForDuration(durationStr || defaultRelativeTime.duration, parsedEnd.toDate());
      }
    }

    return getTimeperiodForDuration(defaultRelativeTime.duration, defaultRelativeTime.until());
  }, [durationStr, endTimeStr, relativeTime]);

  const getCurrentPeriod = useCallback(() => {
    if (!relativeTime) return period;
    const { duration, until } = relativeTime;
    return getTimeperiodForDuration(duration, until());
  }, [relativeTime, period]);

  const refreshPeriod = useCallback(() => {
    if (!relativeTime && endTimeStr) return false;

    setPeriod(
      { nextRelativeTime: relativeTime || defaultRelativeTime },
      { replace: true }
    );

    return true;
  }, [relativeTime, setPeriod, endTimeStr]);

  return {
    period,
    setPeriod,
    refreshPeriod,
    getCurrentPeriod,
    relativeTime: relativeTime || (!endTimeStr ? defaultRelativeTime : null),
    getUrlParams,
  };
};
