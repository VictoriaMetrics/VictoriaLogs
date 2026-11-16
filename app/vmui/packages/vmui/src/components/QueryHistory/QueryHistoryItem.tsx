import { FC } from "preact/compat";
import Button from "../Main/Button/Button";
import { CopyIcon, DeleteIcon, PlayIcon } from "../Main/Icons";
import Tooltip from "../Main/Tooltip/Tooltip";
import useCopyToClipboard from "../../hooks/useCopyToClipboard";
import "./style.scss";
import { QueryHistoryEntry } from "./types";
import { formatHistoryDate } from "./utils";

interface Props {
  entry: QueryHistoryEntry;
  onRun: (query: string) => void;
  onRemove: (query: string) => void;
}

const QueryHistoryItem: FC<Props> = ({ entry, onRun, onRemove }) => {
  const copyToClipboard = useCopyToClipboard();

  const handleCopyQuery = async () => {
    await copyToClipboard(entry.query, "Query has been copied");
  };

  const handleRemoveHistory = () => {
    onRemove(entry.query);
  };

  const handleRunQuery = () => {
    onRun(entry.query);
  };

  return (
    <div className="vm-query-history-item">
      <span className="vm-query-history-item__time">{formatHistoryDate(entry.lastRunAt)}</span>

      <span className="vm-query-history-item__value">{entry.query}</span>

      <div className="vm-query-history-item__buttons">
        <Tooltip title={"Remove from history"}>
          <Button
            size="small"
            variant="text"
            color="gray"
            onClick={handleRemoveHistory}
            startIcon={<DeleteIcon/>}
            aria-label="Remove from history"
          />
        </Tooltip>
        <Tooltip title={"Copy query"}>
          <Button
            size="small"
            variant="text"
            color="gray"
            onClick={handleCopyQuery}
            startIcon={<CopyIcon/>}
            aria-label="Copy query"
          />
        </Tooltip>
        <Button
          variant="text"
          size="small"
          color="primary"
          startIcon={<PlayIcon/>}
          onClick={handleRunQuery}
          aria-label="Run query"
        >
          Run
        </Button>
      </div>
    </div>
  );
};

export default QueryHistoryItem;
