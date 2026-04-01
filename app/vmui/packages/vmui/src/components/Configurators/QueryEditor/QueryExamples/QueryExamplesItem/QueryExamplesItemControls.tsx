import { FC } from "preact/compat";
import { LOGS_DOCS_URL } from "../../../../../constants/logs";
import Button from "../../../../Main/Button/Button";
import { OpenNewIcon, PlayIcon } from "../../../../Main/Icons";
import Tooltip from "../../../../Main/Tooltip/Tooltip";
import { QueryExample } from "../types";

type Props = {
  example: QueryExample;
  onApply: (value: string) => void;
}

const QueryExamplesItemControls: FC<Props> = ({ example, onApply }) => {
  const url = `${LOGS_DOCS_URL}/logsql/#${example.docAnchor || ""}`;

  const handleApplyQuery = () => {
    onApply(example.query);
  };

  return (
    <div className="vm-query-examples-content-item-header-controls">
      <a
        href={url}
        target="_blank"
        rel="noreferrer"
      >
        <Button
          startIcon={<OpenNewIcon/>}
          variant="text"
          size="small"
          color="gray"
        >
          Docs
        </Button>
      </a>

      <Tooltip
        title={"Replace current query and run search"}
        placement="top-right"
      >
        <Button
          variant="text"
          size="small"
          color="primary"
          startIcon={<PlayIcon/>}
          onClick={handleApplyQuery}
        >
          Run
        </Button>
      </Tooltip>
    </div>
  );
};

export default QueryExamplesItemControls;
