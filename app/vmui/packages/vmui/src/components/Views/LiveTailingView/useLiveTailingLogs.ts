import { Dispatch, MutableRefObject, SetStateAction, useCallback, useEffect, useRef, useState } from "preact/compat";
import { ErrorTypes } from "../../../types";
import { Logs } from "../../../api/types";
import { useAppState } from "../../../state/common/StateContext";
import useBoolean from "../../../hooks/useBoolean";
import { useTenant } from "../../../hooks/useTenant";
import { LogFlowAnalyzer } from "./utils";
import useStateSearchParams from "../../../hooks/useStateSearchParams";
import { LIVE_TAILING_OFFSET_PARAM } from "./constants";
import { useExtraFilters } from "../../ExtraFilters/hooks/useExtraFilters";

/**
 * Defines the log's threshold, after which will be shown a warning notification
 */
const LOGS_THRESHOLD = 200;
const CONNECTION_TIMEOUT_MS = 5000;
const PROCESSING_INTERVAL_MS = 1000;

const createStreamProcessor = (
  bufferRef: MutableRefObject<string>,
  bufferLinesRef: MutableRefObject<string[]>,
  restartTailing: () => Promise<boolean>
) => {
  return async (reader: ReadableStreamDefaultReader<Uint8Array>) => {
    let lastDataTime = Date.now();

    const connectionCheckInterval = setInterval(() => {
      const timeSinceLastData = Date.now() - lastDataTime;
      if (timeSinceLastData > CONNECTION_TIMEOUT_MS) {
        clearInterval(connectionCheckInterval);
        void restartTailing();
        return;
      }
    }, CONNECTION_TIMEOUT_MS);

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        lastDataTime = Date.now();

        const chunk = new TextDecoder().decode(value);
        const lines = (bufferRef.current + chunk).split("\n");
        bufferRef.current = lines.pop() || "";
        bufferLinesRef.current = [...bufferLinesRef.current, ...lines];
      }
    } catch (e) {
      if (e instanceof Error && e.name !== "AbortError") {
        console.error("Stream processing error:", e);
        void restartTailing();
      }
    } finally {
      clearInterval(connectionCheckInterval);
    }
  };
};

const parseLogLines = (lines: string[], counterRef: MutableRefObject<bigint>): Logs[] => {
  return lines
    .map(line => {
      try {
        const parsedLine = line && JSON.parse(line);
        if (!parsedLine || typeof parsedLine !== "object") return null;
        parsedLine._log_id = counterRef.current++;
        return parsedLine;
      } catch (e) {
        console.error(`Failed to parse "${line}" to JSON\n`, e);
        return null;
      }
    })
    .filter(Boolean) as Logs[];
};

interface ProcessBufferedLogsParams {
  lines: string[];
  limit: number;
  counterRef: MutableRefObject<bigint>;
  setIsLimitedLogsPerUpdate: (isLimited: boolean) => void;
  setLogs: Dispatch<SetStateAction<Logs[]>>;
  logFlowAnalyzerRef?: MutableRefObject<LogFlowAnalyzer>;
}

const processBufferedLogs = ({
  lines,
  limit,
  counterRef,
  setIsLimitedLogsPerUpdate,
  setLogs,
  logFlowAnalyzerRef
}: ProcessBufferedLogsParams) => {
  const isLimitLogsMode = logFlowAnalyzerRef?.current?.update(lines.length) === "high";
  const limitedLines = isLimitLogsMode && lines.length > LOGS_THRESHOLD ? lines.slice(-LOGS_THRESHOLD) : lines;
  const newLogs = parseLogLines(limitedLines, counterRef);

  setIsLimitedLogsPerUpdate(isLimitLogsMode);
  setLogs(prevLogs => {
    const combinedLogs = [...prevLogs, ...newLogs];
    return combinedLogs.length > limit ? combinedLogs.slice(-limit) : combinedLogs;
  });
};

export const useLiveTailingLogs = (query: string, limit: number) => {
  const { serverUrl } = useAppState();
  const [offset] = useStateSearchParams(5, LIVE_TAILING_OFFSET_PARAM);
  const { extraParams } = useExtraFilters();
  const extraParamsString = extraParams.toString();

  const [logs, setLogs] = useState<Logs[]>([]);
  const { value: isPaused, setTrue: pauseLiveTailing, setFalse: resumeLiveTailing } = useBoolean(false);
  const tenant = useTenant();
  const [error, setError] = useState<ErrorTypes | string>();
  const [isLimitedLogsPerUpdate, setIsLimitedLogsPerUpdate] = useState(false);

  const counterRef = useRef<bigint>(0n);
  const abortControllerRef = useRef(new AbortController());
  const readerRef = useRef<ReadableStreamDefaultReader<Uint8Array> | null>(null);
  const bufferRef = useRef<string>("");
  const bufferLinesRef = useRef<string[]>([]);
  const logFlowAnalyzerRef = useRef(new LogFlowAnalyzer());

  const stopLiveTailing = useCallback(async () => {
    const reader = readerRef.current;
    const abortController = abortControllerRef.current;

    readerRef.current = null;

    if (reader) {
      await reader.cancel();
    }

    if (bufferRef.current) {
      bufferRef.current = "";
    }

    abortController.abort();
  }, []);

  const startLiveTailing = useCallback(async () => {
    await stopLiveTailing();

    abortControllerRef.current = new AbortController();
    const { signal } = abortControllerRef.current;

    setError(undefined);

    try {
      const body = new URLSearchParams({
        query: query.trim(),
        offset: `${offset}s`,
      });

      extraParams.forEach((value, key) => {
        body.append(key, value);
      });

      const response = await fetch(`${serverUrl}/select/logsql/tail`, {
        signal,
        method: "POST",
        headers: {
          ...tenant,
        },
        body,
      });

      if (!response.ok || !response.body) {
        const text = await response.text();
        setError(text);
        setLogs([]);
        return false;
      }

      const reader = response.body.getReader();
      readerRef.current = reader;

      const processStream = createStreamProcessor(
        bufferRef,
        bufferLinesRef,
        startLiveTailing
      );

      void processStream(reader);
      return true;
    } catch (e) {
      if (e instanceof Error && e.name !== "AbortError") {
        setError(String(e));
        console.error(e);
        setLogs([]);
      }
      return false;
    }
  }, [query, stopLiveTailing, offset, extraParamsString]);

  useEffect(() => {
    if (isPaused) {
      const pauseTimerId = setInterval(() => {
        if (bufferLinesRef.current.length > limit) {
          bufferLinesRef.current = bufferLinesRef.current.slice(-limit);
        }
      }, PROCESSING_INTERVAL_MS);
      return () => {
        clearInterval(pauseTimerId);
      };
    }

    const timerId = setInterval(() => {
      const lines = bufferLinesRef.current;
      bufferLinesRef.current = [];

      processBufferedLogs({
        lines,
        limit,
        counterRef,
        setIsLimitedLogsPerUpdate,
        setLogs,
        logFlowAnalyzerRef
      });
    }, PROCESSING_INTERVAL_MS);

    return () => clearInterval(timerId);
  }, [limit, isPaused]);

  const clearLogs = useCallback(() => {
    setLogs([]);
  }, []);

  return {
    logs,
    isPaused,
    error,
    startLiveTailing,
    stopLiveTailing,
    pauseLiveTailing,
    resumeLiveTailing,
    clearLogs,
    isLimitedLogsPerUpdate
  };
};
