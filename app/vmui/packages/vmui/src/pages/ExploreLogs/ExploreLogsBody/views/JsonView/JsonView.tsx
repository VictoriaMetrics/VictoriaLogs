import { FC, useMemo, useCallback, createPortal, memo } from "preact/compat";
import DownloadLogsButton from "../../../DownloadLogsButton/DownloadLogsButton";
import { ViewProps } from "../../types";
import EmptyLogs from "../components/EmptyLogs/EmptyLogs";
import "./style.scss";
import { Logs } from "../../../../../api/types";
import ScrollToTopButton from "../../../../../components/ScrollToTopButton/ScrollToTopButton";
import { CopyButton } from "../../../../../components/CopyButton/CopyButton";
import { JsonView as JsonViewComponent } from "../../../../../components/Views/JsonView/JsonView";

const MemoizedJsonView = memo(JsonViewComponent);

const JsonView: FC<ViewProps> = ({ data, settingsRef }) => {
  const getLogs = useCallback(() => data, [data]);

  const fields = useMemo(() => {
    const keys = new Set(data.flatMap(Object.keys));
    return Array.from(keys);
  }, [data]);

  const orderedFieldsData = useMemo(() => {
    const orderedFields = fields.toSorted((a, b) => a.localeCompare(b));
    return data.map((item) => {
      return orderedFields.reduce((acc, field) => {
        if (item[field]) acc[field] = item[field];
        return acc;
      }, {} as Logs);
    });
  }, [fields, data]);

  const getData = useCallback(() => JSON.stringify(orderedFieldsData, null, 2), [orderedFieldsData]);

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
          <DownloadLogsButton getLogs={getLogs} />
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
        data={orderedFieldsData}
      />
      <ScrollToTopButton />
    </div>
  );
};

export default JsonView;
