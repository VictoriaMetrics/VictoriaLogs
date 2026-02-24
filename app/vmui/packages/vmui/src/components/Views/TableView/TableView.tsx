import { FC } from "preact/compat";
import "./style.scss";
import { ViewProps } from "../../../pages/QueryPage/QueryPageBody/types";
import useStateSearchParams from "../../../hooks/useStateSearchParams";
import EmptyLogs from "../../EmptyLogs/EmptyLogs";
import { useTableColumnView } from "../../Table/hooks/useTableColumnView";
import TableLogsSettings from "./TableLogsSettings";
import TableLogs from "./TableLogs";
import { useTableLogsKeys } from "./hooks/useTableLogsKeys";

const tableId = "table-query-logs";

const TableView: FC<ViewProps> = ({ data, settingsRef }) => {
  const [rowsPerPage, setRowsPerPage] = useStateSearchParams(100, "rows_per_page");

  const { columnKeys, streamKeys, statsByKey } = useTableLogsKeys(data);
  const { viewColumnKeys, dispatchViewColumns } = useTableColumnView(tableId, columnKeys, streamKeys);

  if (!data.length) return <EmptyLogs />;

  return (
    <>
      <TableLogsSettings
        columnKeys={columnKeys}
        viewColumnKeys={viewColumnKeys}
        statsByKey={statsByKey}
        dispatchViewColumns={dispatchViewColumns}
        rowsPerPage={Number(rowsPerPage)}
        setRowsPerPage={setRowsPerPage}
        targetRef={settingsRef}
      />
      <TableLogs
        tableId={tableId}
        logs={data}
        columns={viewColumnKeys}
        rowsPerPage={Number(rowsPerPage)}
        applyViewColumns={dispatchViewColumns}
      />
    </>
  );
};

export default TableView;
