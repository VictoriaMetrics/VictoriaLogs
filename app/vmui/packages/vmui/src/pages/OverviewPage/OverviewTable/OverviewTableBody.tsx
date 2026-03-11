import { FC, ReactNode } from "preact/compat";
import LineLoader from "../../../components/Main/LineLoader/LineLoader";
import Alert from "../../../components/Main/Alert/Alert";
import Table from "../../../components/Table/Table";
import Pagination from "../../../components/Main/Pagination/Pagination";
import { useEffect, useRef } from "react";
import { LogsFieldValues } from "../../../api/types";
import { useTableLogsPaginate } from "../../../components/Views/TableView/hooks/useTableLogsPaginate";
import { type Column } from "../../../components/Table/types";

export type OverviewTableProps = {
  tableId: string;
  rows: LogsFieldValues[]
  columns: Column<LogsFieldValues>[]
  isLoading: boolean;
  error?: string | Error;
  isEmptyList?: boolean;
  emptyListText?: string;
  onClickRow?: (row: LogsFieldValues, e: MouseEvent) => void;
  detectActiveRow?: (row: LogsFieldValues) => boolean;
  actionsRender?: (row: LogsFieldValues) => ReactNode;
}

interface Props extends  OverviewTableProps {
  rowsPerPage: number;
}

const OverviewTableBody: FC<Props> = ({
  tableId,
  rows,
  columns,
  isLoading,
  error,
  rowsPerPage,
  isEmptyList,
  emptyListText,
  onClickRow,
  detectActiveRow,
  actionsRender
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const { page, offset, onChangePage } = useTableLogsPaginate({ rowsPerPage, containerRef });

  useEffect(() => {
    onChangePage(1);
  }, [rows]);

  return (
    <div className="vm-top-fields-body">
      {isLoading && <LineLoader/>}
      {error && <Alert variant="error">{error}</Alert>}

      {isEmptyList && (
        <div className="vm-empty vm-top-fields-body__empty">
          {emptyListText || "The list is empty."}
        </div>
      )}

      {!isEmptyList && !isLoading && (
        <>
          <div
            className="vm-top-fields-body__table"
            ref={containerRef}
          >
            <Table
              tableId={tableId}
              rows={rows}
              columns={columns}
              defaultOrder={{ key: "hits", dir: "desc" }}
              isActiveRow={detectActiveRow}
              onClickRow={onClickRow}
              paginationOffset={offset}
              actionsRender={actionsRender}
            />
          </div>
          <Pagination
            currentPage={page}
            totalItems={rows.length}
            itemsPerPage={rowsPerPage}
            onPageChange={onChangePage}
          />
        </>
      )}
    </div>
  );
};

export default OverviewTableBody;
