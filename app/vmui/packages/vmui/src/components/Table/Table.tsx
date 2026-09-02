import { Fragment, useState, useMemo, useRef, useEffect } from "preact/compat";
import { getComparator, stableSort } from "./helpers";
import { OrderDir } from "../../types";
import TableHeaderCell from "./TableHeaderCell/TableHeaderCell";
import TableCell from "./TableCell/TableCell";
import TableCellActions from "./TableCell/TableCellActions";
import TableRow from "./TableRow/TableRow";
import TableExpandCell from "./TableExpandCell/TableExpandCell";
import "./style.scss";
import { useTableColumnPrefs } from "./hooks/useTableColumnPrefs";
import { Size, useResizeObserver } from "../../hooks/useResizeObserver";
import { useDebounceCallback } from "../../hooks/useDebounceCallback";
import { ColumnKey, TableProps } from "./types";
import { useDragColumn } from "./hooks/useDragColumn";

const Table = <T extends object>({
  tableId,
  rows,
  columns,
  defaultOrder,
  isActiveRow,
  onClickRow,
  actionsRender,
  renderExpandedRow,
  paginationOffset,
  applyViewColumns = () => {
  },
}: TableProps<T>) => {
  const { getColumnPrefs, updateColumnPref } = useTableColumnPrefs(tableId);

  const dragController = useDragColumn({
    axis: "x" as "x" | "y",
    arr: columns.filter(col => col.options.draggable).map(col => String(col.key)),
    onChange: (nextKeys: string[]) => applyViewColumns({ type: "replace", columnKeys: nextKeys })
  });

  const [orderBy, setOrderBy] = useState<ColumnKey<T>>(defaultOrder?.key || columns[0]?.key);
  const [orderDir, setOrderDir] = useState<OrderDir>(defaultOrder?.dir || "desc");

  useEffect(() => {
    setOrderBy(defaultOrder?.key || columns[0]?.key);
    setOrderDir(defaultOrder?.dir || "desc");
  }, [defaultOrder?.key, defaultOrder?.dir]);

  const [offsetStart, offsetEnd] = paginationOffset;

  const sortedList = useMemo(() => {
    return stableSort<T>(rows, getComparator(orderDir, orderBy)).slice(offsetStart, offsetEnd);
  }, [rows, orderBy, orderDir, offsetStart, offsetEnd]);

  const [expandedRows, setExpandedRows] = useState<Set<number>>(new Set());

  useEffect(() => {
    setExpandedRows(new Set());
    // reset on pagination bounds change, not on the paginationOffset tuple identity
    // (the parent may recreate that array every render)
  }, [rows, orderBy, orderDir, offsetStart, offsetEnd]);

  const toggleExpanded = (idx: number) => {
    setExpandedRows(prev => {
      const next = new Set(prev);
      next.has(idx) ? next.delete(idx) : next.add(idx);
      return next;
    });
  };

  const sortPack = useMemo(() => ({
    key: orderBy,
    dir: orderDir,
    onChange: (key: ColumnKey<T>, orderDir: OrderDir) => {
      setOrderDir(orderDir);
      setOrderBy(key);
    },
  }), [orderBy, orderDir]);

  const tableRef = useRef<HTMLTableElement>(null);
  const [size, setSize] = useState<Size>({ width: 0, height: 0 });
  const onResizeContainer = useDebounceCallback(setSize, 200);
  useResizeObserver({ ref: tableRef, onResize: onResizeContainer });

  return (
    <table
      className="vm-table"
      ref={tableRef}
    >
      <thead className="vm-table-header">
        <TableRow variant="header">
          {renderExpandedRow && <th className="vm-table-cell vm-table-cell-header vm-table-cell_expand"/>}
          {columns.map((column, idx) => (
            <TableHeaderCell
              key={column.key}
              idx={idx}
              column={column}
              sort={sortPack}
              prefs={getColumnPrefs(column.key)}
              containerSize={size}
              dragController={dragController}
              applyViewColumns={applyViewColumns}
              onChangePref={updateColumnPref}
            />
        ))}

          {actionsRender && <th className="vm-table-cell vm-table-cell-header vm-table-cell_actions"/>}
          {/* Spacer column fills remaining width */}
          <th className="vm-table-cell vm-table-cell-header_empty"/>
        </TableRow>
      </thead>
      <tbody className="vm-table-body">
        {sortedList.map((row, rowIndex) => (
          <Fragment key={rowIndex}>
            <TableRow
              isActive={isActiveRow && isActiveRow(row as T)}
              onClick={(e) => onClickRow && onClickRow(row as T, e)}
            >
              {renderExpandedRow && (
                <TableExpandCell
                  expanded={expandedRows.has(rowIndex)}
                  onToggle={(e) => {
                    e.stopPropagation();
                    toggleExpanded(rowIndex);
                  }}
                />
              )}

              {columns.map((col) => (
                <TableCell
                  key={String(col.key)}
                  column={col}
                  columnPrefs={getColumnPrefs(col.key)}
                  row={row as T}
                  rowIdx={rowIndex}
                />
            ))}

              {actionsRender && (
                <TableCellActions
                  row={row as T}
                  actionsRender={actionsRender}
                />
              )}

              {/* Spacer column fills remaining width */}
              <td className="vm-table-cell vm-table-cell_empty"/>
            </TableRow>

            {renderExpandedRow && expandedRows.has(rowIndex) && (
              <tr className="vm-table-row vm-table-row_expanded">
                <td
                  className="vm-table-cell vm-table-cell_expanded-content"
                  colSpan={1 + columns.length + (actionsRender ? 1 : 0) + 1}
                >
                  {renderExpandedRow(row as T)}
                </td>
              </tr>
            )}
          </Fragment>
      ))}
      </tbody>
    </table>
  );
};

export default Table;
