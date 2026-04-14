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
import { generatePath, Link, useSearchParams } from "react-router-dom";
import { LOGS_URL_PARAMS } from "../../constants/logs";
import router from "../../router";
import classNames from "classnames";
import "./style.scss";
import { OpenNewIcon } from "../../components/Main/Icons";
import { getStreamPairs } from "../../utils/logs";
import GroupLogsHeaderItem from "../../components/Views/GroupView/GroupLogsHeaderItem";

interface Props {
  log: Logs;
  displayFields?: string[];
  isModal?: boolean; // Indicates if the component is used in a modal
}

const StreamContextList: FC<Props> = ({ log, displayFields, isModal }) => {
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
    abortController
  } = useFetchStreamContext();

  const streamFields = useMemo(() => {
    const stream = logsBefore[0]?._stream || logsAfter[0]?._stream || log._stream || "";
    return getStreamPairs(stream);
  }, [logsBefore, logsAfter, log._stream]);

  const handleLoadMoreAfter = () => {
    const target = logsAfter[0];
    fetchContextLogs({ log: target, linesAfter: loadSize });
  };

  const handleLoadMoreBefore = () => {
    const target = logsBefore[logsBefore.length - 1];
    fetchContextLogs({ log: target, linesBefore: loadSize });
  };

  const handleChangeLoadSize = (limit: number) => {
    setLoadSize(limit);
  };

  const toggleWrapLines = () => {
    searchParams.set(LOGS_URL_PARAMS.NO_WRAP_LINES, String(!noWrapLines));
    setSearchParams(searchParams);
  };

  useEffect(() => {
    fetchContextLogs({ log, linesBefore: 10, linesAfter: 10 });

    return () => {
      resetContextLogs();
      abortController?.abort(); // Abort the fetch request when closing the modal
    };
  }, []);

  const streamPairs = (
    <div className="vm-steam-context-header-streams">
      {streamFields.map(streamPair => (
        <GroupLogsHeaderItem
          key={streamPair}
          pair={streamPair}
        />
      ))}
    </div>
  );

  return (
    <div
      className={classNames({
        "vm-steam-context": true,
        "vm-steam-context_modal": isModal,
      })}
    >
      {isLoading && <LineLoader/>}

      <div className={classNames("vm-steam-context-header", { "vm-steam-context-header_page": !isModal })}>
        {!isModal && (
          <h1 className="vm-modal-content-header__title vm-steam-context-header-title">Log context</h1>
        )}
        {streamPairs}
        {isModal && (
          <div className="vm-steam-context-header__link">
            <Link
              target="_blank"
              to={generatePath(router.streamContext, {
                _stream_id: log._stream_id,
                _time: log._time,
              })}
              rel="noreferrer"
            >
              <Button
                startIcon={<OpenNewIcon/>}
                variant="text"
                size="small"
              >
                Open in new page
              </Button>
            </Link>
          </div>
        )}
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
            variant={!hasMore.after ? "text" : "contained"}
          >
            {!hasMore.after ? "no more logs after" : "Load newer logs"}
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

        {logsBefore.map((log, rowN) => (
          <GroupLogsItem
            isContextView
            hideGroupButton
            key={`${rowN}_${log._time}`}
            log={log}
            displayFields={displayFields}
            className={rowN === 0 ? "vm-steam-context__target-row" : ""}
          />
        ))}
      </div>

      {!error && (
        <div className="vm-steam-context__load-more vm-steam-context__load-more_before">
          <Button
            onClick={handleLoadMoreBefore}
            disabled={isLoading || !hasMore.before}
            variant={!hasMore.before ? "text" : "contained"}
          >
            {!hasMore.before ? "no more logs before" : "Load older logs"}
          </Button>
        </div>
      )}
    </div>
  );
};

export default StreamContextList;
