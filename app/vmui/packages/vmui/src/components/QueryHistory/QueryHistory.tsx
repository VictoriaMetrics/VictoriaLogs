import { FC, useMemo, useState } from "preact/compat";
import Button from "../Main/Button/Button";
import { DeleteIcon, HistoryIcon } from "../Main/Icons";
import useBoolean from "../../hooks/useBoolean";
import Modal from "../Main/Modal/Modal";
import useDeviceDetect from "../../hooks/useDeviceDetect";
import useEventListener from "../../hooks/useEventListener";
import {
  clearQueryHistoryStorage,
  getHistoryFromStorage,
  groupHistoryByDay,
  removeQueryFromHistoryStorage
} from "./utils";
import QueryHistoryItem from "./QueryHistoryItem";
import classNames from "classnames";
import "./style.scss";
import { QueryHistoryGroup } from "./types";

interface Props {
  handleSelectQuery: (query: string) => void
}

const noDataText = "Query history is empty.\nTo see the history, please make a query.";

const QueryHistory: FC<Props> = ({ handleSelectQuery }) => {
  const { isMobile } = useDeviceDetect();

  const {
    value: openModal,
    setTrue: handleOpenModal,
    setFalse: handleCloseModal,
  } = useBoolean(false);

  const [historyStorage, setHistoryStorage] = useState(getHistoryFromStorage());
  const isNoData = !historyStorage.length;

  const groupedData: QueryHistoryGroup[] = useMemo(() => groupHistoryByDay(historyStorage), [historyStorage]);

  const handleRunQuery = (value: string) => {
    handleSelectQuery(value);
    handleCloseModal();
  };

  const handleRemoveHistory = (query: string) => {
    removeQueryFromHistoryStorage(query);
  };

  const updateStageHistory = () => {
    setHistoryStorage(getHistoryFromStorage());
  };

  const handleClearStorage = () => {
    clearQueryHistoryStorage();
  };

  useEventListener("storage", updateStageHistory);

  const Footer = () => (
    <div className="vm-query-history-footer">
      <Button
        color="error"
        variant="text"
        startIcon={<DeleteIcon/>}
        onClick={handleClearStorage}
      >
        Clear all
      </Button>
    </div>
  );

  return (
    <>
      <Button
        color="primary"
        variant="outlined"
        onClick={handleOpenModal}
        startIcon={<HistoryIcon/>}
        aria-label={"Query history"}
      >
        {!isMobile && "History"}
      </Button>

      {openModal && (
        <Modal
          title="Query history"
          className="vm-query-history-modal"
          onClose={handleCloseModal}
          footer={<Footer/>}
        >
          <div
            className={classNames({
              "vm-query-history": true,
              "vm-query-history_mobile": isMobile,
            })}
          >
            <div className="vm-query-history-list">
              {isNoData && <div className="vm-query-history-list__no-data">{noDataText}</div>}

              {groupedData.map((group) => (
                <div
                  key={group.title}
                  className="vm-query-history-list-group"
                >
                  <h3 className="vm-query-history-list-group__title">
                    {group.title}
                    <span>{group.entries.length} {group.entries.length === 1 ? "query" : "queries"}</span>
                  </h3>
                  <div className="vm-query-history-list-group__entries">
                    {group.entries.map((entry) => (
                      <QueryHistoryItem
                        key={`${entry.query}-${entry.lastRunAt}`}
                        entry={entry}
                        onRun={handleRunQuery}
                        onRemove={handleRemoveHistory}
                      />
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </Modal>
      )}
    </>
  );
};

export default QueryHistory;
