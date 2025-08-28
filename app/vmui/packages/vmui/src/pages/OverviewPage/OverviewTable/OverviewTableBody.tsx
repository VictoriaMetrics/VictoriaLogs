import { FC, ReactNode } from "preact/compat";
import LineLoader from "../../../components/Main/LineLoader/LineLoader";
import Alert from "../../../components/Main/Alert/Alert";
import Table, { Column } from "../../../components/Table/Table";
import Pagination from "../../../components/Main/Pagination/Pagination";
import { useEffect, useMemo, useRef, useState } from "react";
import { LogsFiledValues } from "../../../api/types";

export type OverviewTableProps = {
  rows: LogsFiledValues[]
  columns: Column<LogsFiledValues>[]
  isLoading: boolean;
  error?: string | Error;
  isEmptyList?: boolean;
  emptyListText?: string;
  onClickRow?: (row: LogsFiledValues, e: MouseEvent) => void;
  detectActiveRow?: (row: LogsFiledValues) => boolean;
  actionsRender?: (row: LogsFiledValues) => ReactNode;
}

interface Props extends  OverviewTableProps {
  rowsPerPage: number;
}

const OverviewTableBody: FC<Props> = ({
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

  const [page, setPage] = useState(1);
  const paginationOffset = useMemo(() => {
    const startIndex = (page - 1) * rowsPerPage;
    const endIndex = startIndex + rowsPerPage;
    return { startIndex, endIndex };
  }, [page, rowsPerPage]);

  const handlePageChange = (newPage: number) => {
    setPage(newPage);
    if (containerRef.current) {
      const y = containerRef.current.getBoundingClientRect().top + window.scrollY - 50;
      if (y < window.scrollY) window.scrollTo({ top: y });
    }
  };

  useEffect(() => {
    setPage(1);
  }, [rows, rowsPerPage]);

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
              rows={rows}
              columns={columns}
              defaultOrderBy={"hits"}
              defaultOrderDir={"desc"}
              isActiveRow={detectActiveRow}
              onClickRow={onClickRow}
              paginationOffset={paginationOffset}
              actionsRender={actionsRender}
            />
          </div>
          <Pagination
            currentPage={page}
            totalItems={rows.length}
            itemsPerPage={rowsPerPage}
            onPageChange={handlePageChange}
          />
        </>
      )}
    </div>
  );
};

export default OverviewTableBody;
