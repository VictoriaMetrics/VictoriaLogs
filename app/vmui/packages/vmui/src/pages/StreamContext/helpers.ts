import { Logs } from "../../api/types";
import { getDurationFromMilliseconds, getSecondsFromDuration, toNanoPrecision } from "../../utils/time";
import { Direction } from "./hooks/useFetchStreamContext";
import { Dispatch, SetStateAction } from "preact/compat";
import { removeLogsByKeys } from "../../utils/logs";

export const STREAM_CONTEXT_LOAD_SIZE = 30;

export const STREAM_CONTEXT_TIME_WINDOW_INITIAL = "1m";
export const STREAM_CONTEXT_TIME_WINDOW_MAX = "7d";
export const STREAM_CONTEXT_TIME_WINDOW_MULTIPLIER = 2;

/** Checks max time_window by seconds. */
export const isMaxTimeWindow = (timeWindow: string): boolean => {
  return getSecondsFromDuration(timeWindow) >= getSecondsFromDuration(STREAM_CONTEXT_TIME_WINDOW_MAX);
};

/** Normalizes time_window to whole minutes, hours, or days. */
const normalizeTimeWindowSeconds = (seconds: number): number => {
  const minute = 60;
  const hour = 60 * minute;
  const day = 24 * hour;
  const maxSeconds = getSecondsFromDuration(STREAM_CONTEXT_TIME_WINDOW_MAX);

  if (seconds >= maxSeconds) return maxSeconds;

  if (seconds >= day) {
    return Math.floor(seconds / day) * day;
  }

  if (seconds >= hour) {
    return Math.floor(seconds / hour) * hour;
  }

  return Math.floor(seconds / minute) * minute;
};

/** Returns the next time_window for the stream_context pipe. */
export const getNextTimeWindow = (currentWindow: string): string => {
  const currentSeconds = getSecondsFromDuration(currentWindow);
  const maxSeconds = getSecondsFromDuration(STREAM_CONTEXT_TIME_WINDOW_MAX);

  const nextSeconds = Math.min(
    currentSeconds * STREAM_CONTEXT_TIME_WINDOW_MULTIPLIER,
    maxSeconds
  );

  const normalizedSeconds = normalizeTimeWindowSeconds(nextSeconds);

  return getDurationFromMilliseconds(normalizedSeconds * 1000);
};

/** Builds a LogsQL query with the stream_context pipe. */
export const buildContextQuery = (
  log: Logs,
  dir: Direction,
  lines: number,
  timeWindow: string,
): string => {
  const { _stream_id, _time } = log;

  if (!_stream_id || !_time) {
    throw new Error("Log must contain _stream_id and _time fields.");
  }

  return `_stream_id:${_stream_id}
_time:${toNanoPrecision(_time)}
| stream_context ${dir} ${lines} time_window ${timeWindow}`;
};

/** Merges fetched logs and removes the target log. */
export const mergeContextLogs = (dir: Direction, setter: Dispatch<SetStateAction<Logs[]>>) =>
  (fetched: Logs[], target: Logs) => {
    const filtered = removeLogsByKeys(fetched, target, ["_stream_id", "_time"]);
    setter(prev => dir === "after" ? prev.concat(filtered) : filtered.concat(prev));
  };

const MIN_LOG_ROW_HEIGHT = 20;
const INITIAL_LOAD_OVERSCAN = 1.25; // Extra viewport space to ensure initial scroll.

export const getInitialLogsPerSide = (containerHeight: number) => {
  return Math.max(
    STREAM_CONTEXT_LOAD_SIZE,
    Math.ceil((containerHeight * INITIAL_LOAD_OVERSCAN) / MIN_LOG_ROW_HEIGHT),
  );
};
