import { FC, MouseEvent, useState, useEffect } from "preact/compat";
import { useAppState } from "../../../state/common/StateContext";
import Tooltip from "../../Main/Tooltip/Tooltip";
import classNames from "classnames";
import useCopyToClipboard from "../../../hooks/useCopyToClipboard";
import { FieldPair } from "../../../types";
import { convertToFieldFilter, formatFieldPair } from "../../../utils/logs";

interface Props {
  pair: FieldPair;
  withFieldName?: boolean;
  isHide?: boolean;
}

const GroupLogsHeaderItem: FC<Props> = ({ pair, withFieldName, isHide }) => {
  const { isDarkTheme } = useAppState();
  const copyToClipboard = useCopyToClipboard();

  const [copied, setCopied] = useState(false);

  const title = withFieldName || pair.isStream ? formatFieldPair(pair) : pair.value;

  const handleClickByPair = async (e: MouseEvent<HTMLDivElement>) => {
    e.stopPropagation();
    const isCopied = await copyToClipboard(convertToFieldFilter(formatFieldPair(pair)));
    if (isCopied) {
      setCopied(true);
    }
  };

  useEffect(() => {
    if (!copied) return;
    const timeout = setTimeout(() => setCopied(false), 2000);
    return () => clearTimeout(timeout);
  }, [copied]);

  return (
    <Tooltip
      title={copied ? "Copied" : "Copy to clipboard"}
      placement={"top-center"}
    >
      <div
        className={classNames({
          "vm-group-logs-section-keys__pair": true,
          "vm-group-logs-section-keys__pair_hide": isHide,
          "vm-group-logs-section-keys__pair_dark": isDarkTheme
        })}
        onClick={handleClickByPair}
      >
        {title}
      </div>
    </Tooltip>
  );
};

export default GroupLogsHeaderItem;
