import { FC, useCallback, createPortal, memo, useMemo } from "preact/compat";
import { ViewProps } from "../../../pages/QueryPage/QueryPageBody/types";
import EmptyLogs from "../../EmptyLogs/EmptyLogs";
import ScrollToTopButton from "../../ScrollToTopButton/ScrollToTopButton";
import { CopyButton } from "../../CopyButton/CopyButton";
import TreeLogItem from "./TreeLogItem";
import "./style.scss";

const MemoizedTreeLogItem = memo(TreeLogItem);

const TreeLogsView: FC<ViewProps> = ({ data, settingsRef }) => {
  const getData = useCallback(() => JSON.stringify(data, null, 2), [data]);
  const keyedData = useMemo(() => {
    const counts = new Map<string, number>();

    return data.map((log) => {
      const baseKey = JSON.stringify(log);
      const occurrence = counts.get(baseKey) || 0;
      counts.set(baseKey, occurrence + 1);

      return {
        log,
        key: `${baseKey}:${occurrence}`,
      };
    });
  }, [data]);

  const renderSettings = () => {
    if (!settingsRef.current) return null;

    return createPortal(
      data.length > 0 && (
        <div className="vm-json-view__settings-container">
          <CopyButton
            title={"Copy JSON"}
            getData={getData}
            successfulCopiedMessage={"Copied JSON to clipboard"}
          />
        </div>
      ),
      settingsRef.current
    );
  };

  if (!data.length) return <EmptyLogs />;

  return (
    <div className={"vm-tree-logs-view"}>
      {renderSettings()}
      {keyedData.map(({ log, key }) => (
        <MemoizedTreeLogItem
          key={key}
          log={log}
        />
      ))}
      <ScrollToTopButton />
    </div>
  );
};

export default TreeLogsView;
