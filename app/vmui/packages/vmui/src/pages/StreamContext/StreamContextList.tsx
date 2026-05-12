import { FC } from "react";
import { Logs } from "../../api/types";
import { useEffect, useMemo, useState } from "preact/compat";
import { useFetchStreamContext } from "./hooks/useFetchStreamContext";
import GroupLogsItem from "../../components/Views/GroupView/GroupLogsItem";
import LineLoader from "../../components/Main/LineLoader/LineLoader";
import Alert from "../../components/Main/Alert/Alert";
import Button from "../../components/Main/Button/Button";
import SelectLimit from "../../components/Main/Pagination/SelectLimit/SelectLimit";
import Switch from "../../components/Main/Switch/Switch";
import { useSearchParams } from "react-router-dom";
import { LOGS_URL_PARAMS } from "../../constants/logs";
import "./style.scss";
import { getStreamPairs } from "../../utils/logs";
import GroupLogsHeaderItem from "../../components/Views/GroupView/GroupLogsHeaderItem";
import Tooltip from "../../components/Main/Tooltip/Tooltip";

interface Props {
  log: Logs;
  displayFields?: string[];
}

const StreamContextList: FC<Props> = ({ log, displayFields }) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const noWrapLines = searchParams.get(LOGS_URL_PARAMS.NO_WRAP_LINES) === "true";

  const [loadSize, setLoadSize] = useState<number>(10);

  const {
    logsBefore,
    logsAfter,
    hasMore,
    isLoading,
    error,
    fetchContextLogs,
    resetContextLogs,
    abort
  } = useFetchStreamContext();

  const streamFields = useMemo(() => {
    const stream = logsBefore[0]?._stream || logsAfter[0]?._stream || log._stream || "";
    return getStreamPairs(stream);
  }, [logsBefore, logsAfter, log._stream]);

  const handleLoadMoreAfter = () => {
    const target = logsAfter[0];
    if (!target) return;
    void fetchContextLogs({ log: target, linesAfter: loadSize });
  };

  const handleLoadMoreBefore = () => {
    const target = logsBefore[logsBefore.length - 1];
    if (!target) return;
    void fetchContextLogs({ log: target, linesBefore: loadSize });
  };

  const handleChangeLoadSize = (limit: number) => {
    setLoadSize(limit);
  };

  const toggleWrapLines = () => {
    searchParams.set(LOGS_URL_PARAMS.NO_WRAP_LINES, String(!noWrapLines));
    setSearchParams(searchParams);
  };

  useEffect(() => {
    void fetchContextLogs({ log, linesBefore: 10, linesAfter: 10 });

    return () => {
      resetContextLogs();
      abort(); // Abort the fetch request when closing the modal
    };
  }, []);

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
    <div className="vm-steam-context">
      {isLoading && <LineLoader/>}

      <div className="vm-steam-context-header">
        {streamPairs}
        <Switch
          label="Wrap lines"
          value={!noWrapLines}
          onChange={toggleWrapLines}
        />
        <SelectLimit
          limit={loadSize}
          label="Logs per load"
          options={[5, 10, 20, 50, 100]}
          onChange={handleChangeLoadSize}
        />
        <Tooltip title="Time window where stream_context looks for surrounding logs">
          <div className="vm-bar-hits-stats__item">
            Time window
            <b>1h</b>
          </div>
        </Tooltip>

      </div>


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

      {!error && (
        <div className="vm-steam-context__load-more vm-steam-context__load-more_after">
          <Button
            onClick={handleLoadMoreAfter}
            disabled={isLoading || !hasMore.after}
            color={!hasMore.after ? "gray" : "primary"}
            variant={!hasMore.after ? "text" : "contained"}
          >
            {!hasMore.after ? "No logs after within 1h window" : "Load newer logs"}
          </Button>
        </div>
      )}

      <div className="vm-group-logs-section-rows">
        {logsAfter.map((log, rowN) => (
          <GroupLogsItem
            isContextView
            hideGroupButton
            key={`${rowN}_${log._time}`}
            log={log}
            displayFields={displayFields}
          />
        ))}

        <GroupLogsItem
          isContextView
          hideGroupButton
          log={log}
          displayFields={displayFields}
          className="vm-steam-context__target-row"
        />

        {logsBefore.map((log, rowN) => (
          <GroupLogsItem
            isContextView
            hideGroupButton
            key={`${rowN}_${log._time}`}
            log={log}
            displayFields={displayFields}
          />
        ))}
      </div>

      {!error && (
        <div className="vm-steam-context__load-more vm-steam-context__load-more_before">
          <Button
            onClick={handleLoadMoreBefore}
            disabled={isLoading || !hasMore.before}
            color={!hasMore.before ? "gray" : "primary"}
            variant={!hasMore.before ? "text" : "contained"}
          >
            {!hasMore.before ? "No logs before within 1h window" : "Load older logs"}
          </Button>
        </div>
      )}
    </div>
  );
};

export default StreamContextList;
