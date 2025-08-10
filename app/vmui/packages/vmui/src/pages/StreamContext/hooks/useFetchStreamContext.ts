import {
  useState,
  type Dispatch,
  type SetStateAction,
} from "preact/compat";
import { Logs } from "../../../api/types";
import { useFetchLogs } from "../../ExploreLogs/hooks/useFetchLogs";
import { removeExactLog } from "../../../utils/logs";
import { toNanoPrecision } from "../../../utils/time";

type Direction = "before" | "after";

interface FetchParams {
  log: Logs;
  linesBefore?: number;
  linesAfter?: number;
}

const buildContextQuery = (
  log: Logs,
  dir: Direction,
  lines: number
): string => {
  const { _stream_id, _time } = log;

  if (!_stream_id || !_time) {
    throw new Error("Log must contain _stream_id and _time fields.");
  }

  return `_stream_id:${_stream_id} _time:${toNanoPrecision(_time)} | stream_context ${dir} ${lines} | sort by (_time) desc`;
};

const mergeLogs = (dir: Direction, setter: Dispatch<SetStateAction<Logs[]>>) =>
  (fetched: Logs[], target: Logs) => {
    const filtered =  dir === "after" ? removeExactLog(fetched, target) : fetched;
    setter(prev => dir === "after" ? filtered.concat(prev) : prev.concat(filtered));
  };

export const useFetchStreamContext = () => {
  const { fetchLogs, isLoading, error, abortController } = useFetchLogs();

  const [logsBefore, setLogsBefore] = useState<Logs[]>([]);
  const [logsAfter, setLogsAfter] = useState<Logs[]>([]);
  const [hasMore, setHasMore] = useState<{ before: boolean; after: boolean }>({
    before: true,
    after: true,
  });

  const fetchSide = async (
    dir: Direction,
    lines: number,
    setter: Dispatch<SetStateAction<Logs[]>>,
    log: Logs
  ) => {
    if (lines <= 0) return;

    try {
      const data = await fetchLogs({
        query: buildContextQuery(log, dir, lines),
        preventAbortPrevious: true,
      });

      if (Array.isArray(data) && data.length) {
        mergeLogs(dir, setter)(data, log);
        setHasMore(prev => ({
          ...prev,
          [dir]: data.length >= lines,
        }));
      }
    } catch (err) {
      console.error(`Error fetching ${dir} logs:`, err);
    }
  };

  const fetchContextLogs = async ({ log, linesBefore = 0, linesAfter = 0 }: FetchParams) => {
    await Promise.allSettled([
      fetchSide("before", linesBefore, setLogsBefore, log),
      fetchSide("after", linesAfter, setLogsAfter, log),
    ]);
  };

  const resetContextLogs = () => {
    setLogsBefore([]);
    setLogsAfter([]);
    setHasMore({ before: true, after: true });
  };

  return {
    logsBefore,
    logsAfter,
    hasMore,
    isLoading,
    error,
    fetchContextLogs,
    resetContextLogs,
    abortController,
  };
};
