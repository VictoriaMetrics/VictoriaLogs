import { FC, useCallback, createPortal, memo } from "preact/compat";
import { ViewProps } from "../../../pages/QueryPage/QueryPageBody/types";
import EmptyLogs from "../../EmptyLogs/EmptyLogs";
import "./style.scss";
import ScrollToTopButton from "../../ScrollToTopButton/ScrollToTopButton";
import { CopyButton } from "../../CopyButton/CopyButton";
import { JsonView as JsonViewComponent } from "./JsonView";

const MemoizedJsonView = memo(JsonViewComponent);

const JsonLogsView: FC<ViewProps> = ({ data, settingsRef }) => {
  const getData = useCallback(() => JSON.stringify(data, null, 2), [data]);

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
    <div className={"vm-json-view"}>
      {renderSettings()}
      <MemoizedJsonView
        data={data}
      />
      <ScrollToTopButton />
    </div>
  );
};

export default JsonLogsView;
