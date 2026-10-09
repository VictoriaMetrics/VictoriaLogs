import { FC, useCallback, useEffect, useMemo, useRef, useState } from "preact/compat";
import { ViewProps } from "../../../pages/QueryPage/QueryPageBody/types";
import useStateSearchParams from "../../../hooks/useStateSearchParams";
import useSearchParamsFromObject from "../../../hooks/useSearchParamsFromObject";
import "./style.scss";
import { useLiveTailingLogs } from "./useLiveTailingLogs";
import { LOGS_DISPLAY_FIELDS, LOGS_URL_PARAMS } from "../../../constants/logs";
import { useSearchParams } from "react-router-dom";
import throttle from "lodash.throttle";
import GroupLogsItem from "../GroupView/GroupLogsItem";
import LiveTailingSettings from "./LiveTailingSettings";
import Alert from "../../Main/Alert/Alert";
import { isDecreasing } from "../../../utils/array";
import { useLocalStorageBoolean } from "../../../hooks/useLocalStorageBoolean";
import ScrollToTopButton from "../../ScrollToTopButton/ScrollToTopButton";
import { LIVE_TAILING_OFFSET_PARAM } from "./constants";

const SCROLL_THRESHOLD = 100;

const getHeaderBottom = (container: HTMLElement | null): number => {
  const header = container?.closest(".vm-query-page-body")?.querySelector(".vm-query-page-body-header");
  return header ? Math.max(0, header.getBoundingClientRect().bottom) : 0;
};

const LiveTailingView: FC<ViewProps> = ({ settingsRef }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const logsRef = useRef<HTMLDivElement>(null);

  const [isAtBottom, setIsAtBottom] = useState(true);
  const [searchParams] = useSearchParams();
  const { setSearchParamsFromKeys } = useSearchParamsFromObject();
  const [rowsPerPage] = useStateSearchParams(100, "rows_per_page");
  const [offset] = useStateSearchParams(5, LIVE_TAILING_OFFSET_PARAM);
  const [query, _setQuery] = useStateSearchParams("*", LOGS_URL_PARAMS.QUERY);
  const [isRawJsonView, setIsRawJsonView] = useLocalStorageBoolean("RAW_JSON_LIVE_VIEW");
  const {
    logs,
    isPaused,
    error,
    startLiveTailing,
    stopLiveTailing,
    pauseLiveTailing,
    resumeLiveTailing,
    clearLogs,
    isLimitedLogsPerUpdate
  } = useLiveTailingLogs(query, rowsPerPage);

  const displayFieldsString = searchParams.get(LOGS_URL_PARAMS.DISPLAY_FIELDS) || LOGS_DISPLAY_FIELDS;
  const displayFields = useMemo(() => displayFieldsString.split(","), [displayFieldsString]);

  const scrollToBottom = useCallback(() => {
    const logsEl = logsRef.current;
    if (!logsEl) return;

    const rect = logsEl.getBoundingClientRect();
    const headerBottom = getHeaderBottom(containerRef.current);
    if (rect.bottom > headerBottom && rect.bottom <= window.innerHeight) return;

    const scrollY = window.scrollY || document.documentElement.scrollTop;
    const targetScrollTop = Math.max(0, scrollY + rect.bottom - window.innerHeight);

    window.scrollTo({
      top: targetScrollTop,
      // An upward animation can look like manual scrolling when a new batch arrives.
      behavior: targetScrollTop < scrollY ? "instant" : "smooth"
    });
  }, []);

  const throttledScrollToBottom = useMemo(
    () => throttle(scrollToBottom, 200),
    [scrollToBottom]
  );

  useEffect(() => {
    return () => {
      throttledScrollToBottom.cancel();
    };
  }, [throttledScrollToBottom]);

  const handleResumeLiveTailing = useCallback(() => {
    setIsAtBottom(true);
    throttledScrollToBottom();
    resumeLiveTailing();
  }, [resumeLiveTailing, throttledScrollToBottom]);

  const handleSetRowsPerPage = useCallback((limit: number) => {
    setSearchParamsFromKeys({ rows_per_page: limit });
  }, [setSearchParamsFromKeys]);

  const handleSetOffset = useCallback((limit: number) => {
    setSearchParamsFromKeys({ [LIVE_TAILING_OFFSET_PARAM]: limit });
  }, [setSearchParamsFromKeys]);

  useEffect(() => {
    startLiveTailing();
    return () => stopLiveTailing();
  }, [startLiveTailing, stopLiveTailing, offset]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    let prevScrollTop: number[] = [];
    const handleScroll = () => {
      const logsEl = logsRef.current;
      const { scrollTop } = document.documentElement;
      const rect = logsEl?.getBoundingClientRect();
      const headerBottom = getHeaderBottom(containerRef.current);

      const isBottom = !rect || (rect.bottom > headerBottom && rect.bottom <= window.innerHeight + SCROLL_THRESHOLD);

      setIsAtBottom(isBottom);
      prevScrollTop.push(scrollTop);
      prevScrollTop = prevScrollTop.slice(-3);
      const isMoveToTop = isDecreasing(prevScrollTop);

      if (rect && rect.bottom > window.innerHeight + SCROLL_THRESHOLD && !isPaused && isMoveToTop) {
        pauseLiveTailing();
      }
    };

    document.addEventListener("scroll", handleScroll);
    return () => document.removeEventListener("scroll", handleScroll);
  }, [isPaused, pauseLiveTailing]);

  useEffect(() => {
    if (isAtBottom && !isPaused && logs.length > 0) {
      throttledScrollToBottom();
    }
  }, [logs, isAtBottom, isPaused, throttledScrollToBottom]);

  useEffect(() => {
    handleResumeLiveTailing();
  }, [rowsPerPage, offset, handleResumeLiveTailing]);

  if (error) {
    return <div className="vm-live-tailing-view__error">{error}</div>;
  }

  return (
    <>
      <LiveTailingSettings
        settingsRef={settingsRef}
        rowsPerPage={rowsPerPage}
        handleSetRowsPerPage={handleSetRowsPerPage}
        logs={logs}
        isPaused={isPaused}
        handleResumeLiveTailing={handleResumeLiveTailing}
        pauseLiveTailing={pauseLiveTailing}
        clearLogs={clearLogs}
        isRawJsonView={isRawJsonView}
        onRawJsonViewChange={setIsRawJsonView}
        offset={offset}
        handleSetOffset={handleSetOffset}
      />
      <ScrollToTopButton />
      <div
        ref={containerRef}
        className="vm-live-tailing-view__container"
      >
        {logs.length === 0
          ? (<div className="vm-live-tailing-view__empty">Waiting for logs...</div>)
          : (<div
              ref={logsRef}
              className="vm-live-tailing-view__logs"
          >
            {logs.map(({ _log_id, ...log }, idx) =>
              isRawJsonView ? (
                <pre
                  key={idx}
                  className="vm-live-tailing-view__log-row"
                  onMouseDown={pauseLiveTailing}
                >
                  {JSON.stringify(log)}
                </pre>
              ) : (
                <GroupLogsItem
                  key={_log_id}
                  log={log}
                  onItemClick={pauseLiveTailing}
                  hideGroupButton={true}
                  displayFields={displayFields}
                />
              )
            )}
          </div>
          )}
      </div>
      {isLimitedLogsPerUpdate && (
        <Alert
          title="Too many logs per second detected"
          variant="warning"
        >
          Large volumes of log data are difficult to process and may impact performance.
          We recommend adding filters to your query for better analysis and system performance.
        </Alert>)}
    </>
  );
};

export default LiveTailingView;
