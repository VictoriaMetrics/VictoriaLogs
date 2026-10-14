import { FC } from "preact/compat";
import "./style.scss";
import { useSearchParams } from "react-router-dom";
import { ViewProps } from "../../../pages/QueryPage/QueryPageBody/types";
import EmptyLogs from "../../EmptyLogs/EmptyLogs";
import { useTableColumnView } from "../../Table/hooks/useTableColumnView";
import TableLogsSettings from "./TableLogsSettings";
import TableLogs from "./TableLogs";
import { useTableLogsKeys } from "./hooks/useTableLogsKeys";
import { LOGS_URL_PARAMS } from "../../../constants/logs";

const tableId = "table-query-logs";

const TableView: FC<ViewProps> = ({ data, settingsRef }) => {
  const [searchParams] = useSearchParams();

  const rowsPerPageRaw = searchParams.get(LOGS_URL_PARAMS.ROWS_PER_PAGE);
  const rowsPerPageNum = rowsPerPageRaw ? Number(rowsPerPageRaw) : 100;
  const rowsPerPage = isNaN(rowsPerPageNum) ? 0 : rowsPerPageNum;

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
        rowsPerPage={rowsPerPage}
        targetRef={settingsRef}
      />
      <TableLogs
        tableId={tableId}
        logs={data}
        columns={viewColumnKeys}
        rowsPerPage={rowsPerPage}
        applyViewColumns={dispatchViewColumns}
      />
    </>
  );
};

export default TableView;
