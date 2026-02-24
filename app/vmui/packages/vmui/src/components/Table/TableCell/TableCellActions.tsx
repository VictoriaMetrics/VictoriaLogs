import { ReactNode } from "preact/compat";
import "./style.scss";

interface Props<T> {
  row: T;
  actionsRender: (row: T) => ReactNode
}

const TableCellActions = <T extends object>({ row, actionsRender }: Props<T>) => {
  return (
    <td className="vm-table-cell vm-table-cell_no-padding vm-table-cell_right vm-table-cell_actions">
      {actionsRender(row as T)}
    </td>
  );
};

export default TableCellActions;
