import {
  useState,
  type Dispatch,
  type SetStateAction,
} from "preact/compat";
import { Logs } from "../../../api/types";
import { useFetchLogs } from "../../QueryPage/hooks/useFetchLogs";
import {
  appendUniqueContextLogs, buildContextQuery, getNextContextTarget, getNextTimeWindow, isMaxTimeWindow, mergeContextLogs,
  STREAM_CONTEXT_TIME_WINDOW_INITIAL
} from "../helpers";

export type Direction = "before" | "after";

interface FetchParams {
  log: Logs;
  linesBefore?: number;
  linesAfter?: number;
}

interface FetchSideParams {
  dir: Direction;
  lines: number;
  setter: Dispatch<SetStateAction<Logs[]>>;
  log: Logs;
}

export const useFetchStreamContext = () => {
  const { fetchLogs, error, abort } = useFetchLogs();

  const [logsBefore, setLogsBefore] = useState<Logs[]>([]);
  const [logsAfter, setLogsAfter] = useState<Logs[]>([]);
  const [hasMore, setHasMore] = useState<{ before: boolean; after: boolean }>({
    before: true,
    after: true,
  });

  const [isLoading, setIsLoading] = useState<{ before: boolean; after: boolean }>({
    before: false,
    after: false,
  });

  const fetchWithExpandedTimeWindow = async ({ log, dir, lines }: FetchSideParams) => {
    let timeWindow = STREAM_CONTEXT_TIME_WINDOW_INITIAL;
    let accumulatedData: Logs[] = [];
    let target = log;

    while (true) {
      const remainingLines = Math.max(lines - accumulatedData.length, 1);
      const data = await fetchLogs({
        query: buildContextQuery(target, dir, remainingLines, timeWindow),
      });

      if (!Array.isArray(data)) {
        return { data: accumulatedData, timeWindow };
      }

      accumulatedData = appendUniqueContextLogs(accumulatedData, data);

      if (accumulatedData.length >= lines || isMaxTimeWindow(timeWindow)) {
        return { data: accumulatedData, timeWindow };
      }

      target = getNextContextTarget(data, dir) || target;
      timeWindow = getNextTimeWindow(timeWindow);
    }
  };

  const fetchSide = async (params: FetchSideParams) => {
    const { log, lines, dir, setter } = params;

    if (lines <= 0) return;

    setIsLoading(prev => ({
      ...prev,
      [dir]: true,
    }));

    try {
      const { data } = await fetchWithExpandedTimeWindow(params);

      if (data.length) {
        mergeContextLogs(dir, setter)(data, log);
      }

      setHasMore(prev => ({
        ...prev,
        [dir]: data.length >= lines,
      }));
    } catch (err) {
      console.error(`Error fetching ${dir} logs:`, err);
    } finally {
      setIsLoading(prev => ({
        ...prev,
        [dir]: false,
      }));
    }
  };

  const fetchContextLogs = async ({ log, linesBefore = 0, linesAfter = 0 }: FetchParams) => {
    await Promise.allSettled([
      fetchSide({ dir: "before", lines: linesBefore, setter: setLogsBefore, log }),
      fetchSide({ dir: "after", lines: linesAfter, setter: setLogsAfter, log }),
    ]);
  };

  const resetContextLogs = () => {
    setLogsBefore([]);
    setLogsAfter([]);
    setHasMore({ before: true, after: true });
    setIsLoading({ before: false, after: false });
  };

  return {
    logsBefore,
    logsAfter,
    hasMore,
    isLoading,
    error,
    fetchContextLogs,
    resetContextLogs,
    abort,
  };
};
