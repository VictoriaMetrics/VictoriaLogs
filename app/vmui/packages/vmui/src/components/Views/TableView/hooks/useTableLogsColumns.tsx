import { Logs } from "../../../../api/types";
import { useMemo } from "preact/compat";
import TableCopyButton from "../../../Table/TableCopyButton/TableCopyButton";
import { formatDateWithNanoseconds } from "../../../../utils/time";
import { LOGS_DATE_FORMAT } from "../../../../constants/logs";
import { type Column } from "../../../Table/types";

type Options = {
  keys: readonly string[];
}

const getRenderColumnByKey = (key: string): Column<Logs>["render"] => {
  switch (key) {
    case "_time":
      return (log: Logs) => log._time ? formatDateWithNanoseconds(log._time, LOGS_DATE_FORMAT) : "";
    default:
      return undefined;
  }
};

const getCopyData = (log: Logs, keys: Options["keys"]) => () => {
  const logToCopy = Object.fromEntries(keys.map(k => [k, log[k] ?? "-"]));
  return JSON.stringify(logToCopy, null, 2);
};

const getCopyColumn = (keys: Options["keys"]): Column<Logs> => ({
  title: "",
  key: "_vmui_copy",
  options: { sortable: false, resizable: false, draggable: false, menuEnabled: false },
  className: "vm-table-cell_no-padding",
  render: (log: Logs) => <TableCopyButton getData={getCopyData(log, keys)}/>
});

const getBaseColumn = (key: string): Column<Logs> => ({
  key,
  title: key,
  options: { sortable: true, resizable: true, draggable: true, menuEnabled: true },
  render: getRenderColumnByKey(key)
});

export const useTableLogsColumns = ({ keys }: Options) => {
  const tableColumns = useMemo(() => {
    const baseCols = keys.map(getBaseColumn);
    const copyCol = getCopyColumn(keys);
    return [...baseCols, copyCol];
  }, [keys]);

  return {
    tableColumns,
  };
};
