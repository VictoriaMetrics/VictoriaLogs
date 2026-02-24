import { FC, useEffect, useRef } from "preact/compat";
import Table from "../../Table/Table";
import { Logs } from "../../../api/types";
import Pagination from "../../Main/Pagination/Pagination";
import { useTableLogsColumns } from "./hooks/useTableLogsColumns";
import { useTableLogsPaginate } from "./hooks/useTableLogsPaginate";
import { ViewColumnsAction } from "../../Table/hooks/useTableColumnView";

interface TableLogsProps {
  tableId: string;
  logs: Logs[];
  columns: string[];
  rowsPerPage: number;
  applyViewColumns: (action: ViewColumnsAction) => void;
}

const TableLogs: FC<TableLogsProps> = ({ tableId, logs, columns, rowsPerPage, applyViewColumns }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const { page, offset, onChangePage } = useTableLogsPaginate({ rowsPerPage, containerRef });
  const { tableColumns } = useTableLogsColumns({ keys: columns });

  useEffect(() => {
    onChangePage(1);
  }, [logs]);

  return (
    <>
      <div ref={containerRef}/>
      <Table
        tableId={tableId}
        rows={logs}
        columns={tableColumns}
        defaultOrder={{ key: "_time", dir: "desc" }}
        paginationOffset={offset}
        applyViewColumns={applyViewColumns}
      />
      <Pagination
        currentPage={page}
        totalItems={logs.length}
        itemsPerPage={rowsPerPage}
        onPageChange={onChangePage}
      />
    </>
  );
};

export default TableLogs;
