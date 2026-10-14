import { Logs } from "../../api/types";
import { Direction } from "./hooks/useFetchStreamContext";
import { Dispatch, SetStateAction } from "preact/compat";
import { vmDate } from "../../utils/time";

export const STREAM_CONTEXT_LOAD_SIZE = 30;

/** Builds a LogsQL query for loading stream logs around the target timestamp. */
export const buildContextQuery = (
  log: Logs,
  dir: Direction,
  lines: number,
): string => {
  const { _stream_id, _time } = log;
  const sortDir = dir === "before" ? "desc" : "asc";
  const timeComparator = dir === "before" ? "<=" : ">=";

  if (!_stream_id || !_time) {
    throw new Error("Log must contain _stream_id and _time fields.");
  }

  const streamFilter = `_stream_id:${_stream_id}`;
  const timeFilter = `_time:${timeComparator}${vmDate(_time).nano().toISOString()}`;
  const sortPipe = `sort by (_time) ${sortDir}`;
  const limitPipe = `limit ${lines}`;

  return `${streamFilter} ${timeFilter} | ${sortPipe} ${limitPipe}`;
};

const removeAnchorOnce = (logs: Logs[], target: Logs): Logs[] => {
  let removed = false;

  return logs.filter(log => {
    const isAnchor = log._stream_id === target._stream_id && log._time === target._time;

    // _stream_id + _time identify the anchor boundary, but not a unique log entry.
    // Remove a single matching row, so logs with the same timestamp remain visible.
    if (!removed && isAnchor) {
      removed = true;
      return false;
    }

    return true;
  });
};

/** Merges fetched logs and removes a single anchor log. */
export const mergeContextLogs = (dir: Direction, setter: Dispatch<SetStateAction<Logs[]>>) =>
  (fetched: Logs[], target: Logs) => {
    const filtered = removeAnchorOnce(fetched, target);
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
