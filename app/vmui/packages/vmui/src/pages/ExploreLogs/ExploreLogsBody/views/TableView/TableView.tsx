import { FC, memo, useMemo, useCallback, useState } from "preact/compat";
import DownloadLogsButton from "../../../DownloadLogsButton/DownloadLogsButton";
import { createPortal } from "preact/compat";
import "./style.scss";
import { ViewProps } from "../../types";
import useStateSearchParams from "../../../../../hooks/useStateSearchParams";
import TableLogs from "../../TableLogs";
import SelectLimit from "../../../../../components/Main/Pagination/SelectLimit/SelectLimit";
import TableSettings from "../../../../../components/Table/TableSettings/TableSettings";
import useSearchParamsFromObject from "../../../../../hooks/useSearchParamsFromObject";
import EmptyLogs from "../components/EmptyLogs/EmptyLogs";

const MemoizedTableView = memo(TableLogs);

const TableView: FC<ViewProps> = ({ data, settingsRef }) => {
  const { setSearchParamsFromKeys } = useSearchParamsFromObject();
  const [displayColumns, setDisplayColumns] = useState<string[]>([]);
  const [rowsPerPage, setRowsPerPage] = useStateSearchParams(100, "rows_per_page");

  const columns = useMemo(() => {
    const keys = new Set<string>();
    for (const item of data) {
      for (const key in item) {
        keys.add(key);
      }
    }
    return Array.from(keys).sort((a,b) => a.localeCompare(b));
  }, [data]);

  const handleSetRowsPerPage = (limit: number) => {
    setRowsPerPage(limit);
    setSearchParamsFromKeys({ rows_per_page: limit });
  };

  const getLogs = useCallback(() => data, [data]);

  const renderSettings = () => {
    if (!settingsRef.current) return null;

    return createPortal(
      <div className="vm-table-view__settings">
        <SelectLimit
          limit={rowsPerPage}
          onChange={handleSetRowsPerPage}
        />
        <div className="vm-table-view__settings-buttons">
          {data.length > 0 && <DownloadLogsButton getLogs={getLogs} />}
          <TableSettings
            columns={columns}
            selectedColumns={displayColumns}
            onChangeColumns={setDisplayColumns}
          />
        </div>
      </div>,
      settingsRef.current
    );
  };

  if (!data.length) return <EmptyLogs />;

  return (
    <>
      {renderSettings()}
      <MemoizedTableView
        logs={data}
        displayColumns={displayColumns}
        tableCompact={false}
        columns={columns}
        rowsPerPage={Number(rowsPerPage)}
      />
    </>
  );
};

export default TableView;
