import classNames from "classnames";
import "./style.scss";
import { ColumnPrefs } from "../hooks/useTableColumnPrefs";
import { type Column } from "../types";

interface Props<T> {
  row: T;
  rowIdx: number;
  column: Column<T>;
  columnPrefs?: ColumnPrefs;
}

const TableCell = <T extends object>({
  row,
  rowIdx,
  column,
  columnPrefs = {}
}: Props<T>) => {
  const { width, wrapped } = columnPrefs;

  const className = classNames({
    "vm-table-cell": true,
    "vm-table-cell_wrapped": wrapped,
    [`${column.className}`]: column.className
  });

  return (
    <td className={className}>
      <div
        className="vm-table-cell__content"
        style={width ? { width: `${width}px`, maxWidth: "none" } : undefined}
      >
        {column.render ? column.render(row, rowIdx) : row[column.key] ?? "-"}
      </div>
    </td>
  );
};

export default TableCell;
