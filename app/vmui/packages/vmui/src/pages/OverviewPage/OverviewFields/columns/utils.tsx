import { type Column } from "../../../../components/Table/types";
import { LogsFiledValues } from "../../../../api/types";
import { formatNumber } from "../../../../utils/number";

const options = {
  sortable: true,
  draggable: false,
  resizable: false,
  menuEnabled: false
};

export const getFieldCol = (title: string): Column<LogsFiledValues> => ({
  title,
  options,
  classNameHeader: "vm-table-cell_full",
  key: "value" as keyof LogsFiledValues,
});

export const getHitsCol = () => ({
  title: "Hits",
  options,
  className: "vm-table-cell_number",
  classNameHeader: "vm-table-cell-header_number",
  key: "hits" as keyof LogsFiledValues,
  render: (n: LogsFiledValues) => formatNumber(n.hits),
});

export const getPercentCol = (title: string) => ({
  title,
  options,
  className: "vm-table-cell_number",
  classNameHeader: "vm-table-cell-header_number",
  key: "percent" as keyof LogsFiledValues,
  render: (n: LogsFiledValues) => {
    const p = n.percent ?? 0;
    const text = p.toFixed(2);
    return `${text}%`;
  },
});
