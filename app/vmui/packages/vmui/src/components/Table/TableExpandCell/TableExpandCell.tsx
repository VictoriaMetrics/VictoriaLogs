import { FC, MouseEvent } from "preact/compat";
import classNames from "classnames";
import { ArrowDownIcon } from "../../Main/Icons";

interface TableExpandCellProps {
  expanded: boolean;
  onToggle: (e: MouseEvent<HTMLElement>) => void;
}

// The whole cell toggles, not just the chevron: the button is a 14px icon and the
// row around it is a click target for onClickRow, so anything less made expanding
// fiddly.
const TableExpandCell: FC<TableExpandCellProps> = ({ expanded, onToggle }) => (
  <td
    className="vm-table-cell vm-table-cell_expand"
    onClick={onToggle}
  >
    <button
      type="button"
      aria-label={expanded ? "Collapse row" : "Expand row"}
      aria-expanded={expanded}
      className={classNames({
        "vm-table__expand-btn": true,
        "vm-table__expand-btn_open": expanded,
      })}
    >
      <ArrowDownIcon/>
    </button>
  </td>
);

export default TableExpandCell;
