import { FC } from "preact/compat";
import { ArrowDownIcon } from "../../../Main/Icons";
import classNames from "classnames";
import "./style.scss";

interface Props {
  isSorted: boolean;
  isDesc: boolean
}

const TableSortButton: FC<Props> = ({ isSorted, isDesc }) => {
  return (
    <div
      className={classNames({
        "vm-table-sort-button": true,
        "vm-table-sort-button_sorted": isSorted,
        "vm-table-sort-button_desc": isDesc && isSorted,
      })}
    >
      <ArrowDownIcon/>
    </div>
  );
};

export default TableSortButton;
