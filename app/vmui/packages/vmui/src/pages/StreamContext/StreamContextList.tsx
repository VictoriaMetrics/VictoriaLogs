import { FC, useCallback, useLayoutEffect, useMemo, useRef } from "preact/compat";
import { Logs } from "../../api/types";
import { Direction, useFetchStreamContext } from "./hooks/useFetchStreamContext";
import GroupLogsItem from "../../components/Views/GroupView/GroupLogsItem";
import Alert from "../../components/Main/Alert/Alert";
import Switch from "../../components/Main/Switch/Switch";
import { useSearchParams } from "react-router-dom";
import { LOGS_URL_PARAMS } from "../../constants/logs";
import "./style.scss";
import { getStreamPairs } from "../../utils/logs";
import GroupLogsHeaderItem from "../../components/Views/GroupView/GroupLogsHeaderItem";
import { groupByMultipleKeys } from "../../utils/array";
import classNames from "classnames";
import {
  getInitialLogsPerSide,
  STREAM_CONTEXT_LOAD_SIZE
} from "./helpers";
import useDeviceDetect from "../../hooks/useDeviceDetect";
import { scrollElementToCenter } from "../../utils/dom-geometry";
import { useStreamContextScroll } from "./hooks/useStreamContextScroll";
import Button from "../../components/Main/Button/Button";
import { SpinnerIcon } from "../../components/Main/Icons";
import LineLoader from "../../components/Main/LineLoader/LineLoader";

interface Props {
  log: Logs;
  displayFields?: string[];
}

const StreamContextList: FC<Props> = ({ log, displayFields }) => {
  const { isMobile } = useDeviceDetect();

  const [searchParams, setSearchParams] = useSearchParams();
  const noWrapLines = searchParams.get(LOGS_URL_PARAMS.NO_WRAP_LINES) === "true";

  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const targetRowRef = useRef<HTMLDivElement>(null);

  const {
    logsBefore,
    logsAfter,
    hasMore,
    isLoading: { after: isLoadingAfter, before: isLoadingBefore },
    error,
    fetchContextLogs,
    resetContextLogs,
    abort
  } = useFetchStreamContext();

  const logsWithTimeDuplicates = useMemo(() => {
    const logs = logsBefore.concat(log).concat(logsAfter);
    const groupedByTime = groupByMultipleKeys(logs, ["_time"]);
    const groupWithDuplicates = groupedByTime.filter(group => group.values.length > 1);

    // Keep duplicated logs so we can highlight them in the list.
    return groupWithDuplicates.flatMap(group => group.values);
  }, [log, logsBefore, logsAfter]);

  const hasTimeDuplicates = logsWithTimeDuplicates.length > 0;

  const streamFields = useMemo(() => {
    const stream = logsBefore[0]?._stream || logsAfter[0]?._stream || log._stream || "";
    return getStreamPairs(stream);
  }, [logsBefore, logsAfter, log._stream]);

  const handleLoadMore = useCallback((dir: Direction) => {
    const isAfter = dir === "after";

    if (isAfter && (isLoadingAfter || !hasMore.after)) return;
    if (!isAfter && (isLoadingBefore || !hasMore.before)) return;

    const target = isAfter ? logsAfter[logsAfter.length - 1] : logsBefore[0];

    if (!target) return;

    const scrollContainer = scrollContainerRef.current;
    const previousScrollHeight = scrollContainer?.scrollHeight ?? 0;
    const previousScrollTop = scrollContainer?.scrollTop ?? 0;

    void fetchContextLogs({
      log: target,
      linesAfter: isAfter ? STREAM_CONTEXT_LOAD_SIZE : 0,
      linesBefore: isAfter ? 0 : STREAM_CONTEXT_LOAD_SIZE,
    }).then(() => {
      if (isAfter || !scrollContainer) return;

      requestAnimationFrame(() => {
        const isStillAtTop = scrollContainer.scrollTop <= 16;

        if (!isStillAtTop) {
          // User has already scrolled away while older logs were loading, so don't force the viewport back.
          return;
        }

        const offset = 54;
        scrollContainer.scrollTop = scrollContainer.scrollHeight - previousScrollHeight + previousScrollTop - offset;
      });
    });
  }, [fetchContextLogs, hasMore.after, hasMore.before, isLoadingAfter, isLoadingBefore, logsAfter, logsBefore]);

  const toggleWrapLines = () => {
    searchParams.set(LOGS_URL_PARAMS.NO_WRAP_LINES, String(!noWrapLines));
    setSearchParams(searchParams);
  };

  const scrollToTargetLog = () => {
    const scrollContainer = scrollContainerRef.current;
    const targetRow = targetRowRef.current;

    if (!scrollContainer || !targetRow) return;

    scrollElementToCenter(scrollContainer, targetRow);
  };

  useLayoutEffect(() => {
    const scrollContainer = scrollContainerRef.current;
    if (!scrollContainer || !log) return;

    const initialLogsPerSide = getInitialLogsPerSide(scrollContainer.clientHeight);

    void fetchContextLogs({
      log,
      linesBefore: initialLogsPerSide,
      linesAfter: initialLogsPerSide
    }).then(() => {
      // Center the target log after initial context load.
      requestAnimationFrame(scrollToTargetLog);
    });

    return () => {
      resetContextLogs();
      abort(); // Abort the fetch request when closing the modal
    };
  }, [log]);

  const { handleScroll } = useStreamContextScroll({ handleLoadMore });

  const streamPairs = (
    <div className="vm-steam-context-header-streams">
      <b>Stream labels:</b>
      {streamFields.map(streamPair => (
        <GroupLogsHeaderItem
          key={streamPair}
          pair={streamPair}
        />
      ))}
    </div>
  );

  return (
    <div className={classNames("vm-steam-context", { "vm-steam-context_mobile": isMobile })}>
      <div className="vm-steam-context-header">
        {(isLoadingAfter || isLoadingBefore) && <LineLoader/>}

        {streamPairs}

        <Switch
          label="Wrap lines"
          value={!noWrapLines}
          onChange={toggleWrapLines}
        />

        {hasTimeDuplicates && (
          <div className="vm-steam-context-header__warning">
            <Alert
              title="Logs with identical timestamps detected"
              variant="warning"
            >
              Stream context cannot reliably determine the order of log entries
              because some entries share identical timestamps.
            </Alert>
          </div>
        )}

        {error && (
          <div className="vm-steam-context__error">
            <Alert
              title="Failed to load log context"
              variant="error"
            >
              {error}
            </Alert>
          </div>
        )}
      </div>

      <div
        className="vm-group-logs-section-rows vm-steam-context-rows"
        ref={scrollContainerRef}
        onScroll={handleScroll}
      >
        {!error && !hasMore.before && (
          <div className="vm-steam-context__no-load-more">
            {"No older logs"}
          </div>
        )}

        {isLoadingBefore && (
          <div className="vm-steam-context__loader">
            <Button
              startIcon={<SpinnerIcon/>}
              variant="text"
            >
              Loading older logs...
            </Button>
          </div>
        )}

        {logsBefore.map((log, rowN) => (
          <div key={`${rowN}_${log._time}`}>
            <GroupLogsItem
              className={classNames({ "vm-group-logs-section-rows__time-duplicate": logsWithTimeDuplicates.includes(log) })}
              isContextView
              hideGroupButton
              log={log}
              displayFields={displayFields}
            />
          </div>
        ))}

        <div ref={targetRowRef}>
          <GroupLogsItem
            isContextView
            hideGroupButton
            log={log}
            displayFields={displayFields}
            className={classNames("vm-steam-context__target-row", {
              "vm-group-logs-section-rows__time-duplicate": logsWithTimeDuplicates.includes(log),
            })}
          />
        </div>

        {logsAfter.map((log, rowN) => (
          <div key={`${rowN}_${log._time}`}>
            <GroupLogsItem
              className={classNames({ "vm-group-logs-section-rows__time-duplicate": logsWithTimeDuplicates.includes(log) })}
              isContextView
              hideGroupButton
              log={log}
              displayFields={displayFields}
            />
          </div>
        ))}

        {isLoadingAfter && (
          <div className="vm-steam-context__loader">
            <Button
              startIcon={<SpinnerIcon/>}
              variant="text"
            >
              Loading newer logs...
            </Button>
          </div>
        )}

        {!error && !hasMore.after && (
          <div className="vm-steam-context__no-load-more">
            {"No newer logs"}
          </div>
        )}
      </div>
    </div>
  );
};

export default StreamContextList;
