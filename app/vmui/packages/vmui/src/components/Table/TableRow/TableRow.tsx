import { FC, ReactNode } from "preact/compat";
import classNames from "classnames";
import "./style.scss";

interface Props {
  children: ReactNode
  variant?: "header" | "body";
  isActive?: boolean;
  onClick?: (e: MouseEvent) => void;
}

const TableRow: FC<Props> = ({
  children,
  variant = "body",
  isActive,
  onClick,
}) => {
  const isHeader = variant === "header";

  const className = classNames({
    "vm-table-row": true,
    "vm-table-row_header": isHeader,
    "vm-table-row_active": isActive,
    "vm-table-row_clickable": Boolean(onClick)
  });

  return (
    <tr
      className={className}
      onClick={onClick}
    >
      {children}
    </tr>
  );
};

export default TableRow;
