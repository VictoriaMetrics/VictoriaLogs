import { FC, ReactNode } from "preact/compat";
import useBoolean from "../../../hooks/useBoolean";
import OverviewTableHeader from "./OverviewTableHeader";
import OverviewTableBody, { OverviewTableProps } from "./OverviewTableBody";
import { useMemo, useState } from "react";
import TextField from "../../../components/Main/TextField/TextField";
import Tooltip from "../../../components/Main/Tooltip/Tooltip";
import { QuestionIcon } from "../../../components/Main/Icons";
import { OrderDir } from "../../../types";
import { ColumnKey } from "../../../components/Table/types";
import { LogsFieldValues } from "../../../api/types";

interface Props extends OverviewTableProps {
  title: ReactNode | string;
  enableSearch?: boolean;
  headerControls?: ReactNode;
  defaultOrder?: { key?: ColumnKey<LogsFieldValues>; dir?: OrderDir };
}

const OverviewTable: FC<Props> = ({
  tableId,
  title,
  rows,
  enableSearch,
  headerControls,
  defaultOrder,
  ...props
}) => {
  const [rowsPerPage, setRowsPerPage] = useState(10);

  const { value: showSearch, toggle: handleToggleShowSearch } = useBoolean(false);
  const [searchValue, setSearchValue] = useState("");

  const filteredRows = useMemo(() => {
    if (!searchValue) return rows;
    return rows.filter(({ value }) => value.includes(searchValue));
  }, [searchValue, rows]);

  return (
    <div className="vm-top-fields">
      <OverviewTableHeader
        title={title}
        rowsPerPage={rowsPerPage}
        onChangeRowsPerPage={setRowsPerPage}
        enableSearch={enableSearch}
        headerControls={headerControls}
        onToggleShowSearch={handleToggleShowSearch}
      />

      {showSearch && (
        <div className="vm-top-fields__search">
          <TextField
            autofocus
            label="Search"
            value={searchValue}
            onChange={setSearchValue}
            endIcon={(
              <Tooltip title={"Filters rows in the current table only. Does not send queries to the server."}>
                <QuestionIcon/>
              </Tooltip>
            )}
          />
        </div>
      )}

      <OverviewTableBody
        {...props}
        tableId={tableId}
        rows={filteredRows}
        rowsPerPage={rowsPerPage}
        defaultOrder={defaultOrder}
      />
    </div>
  );
};

export default OverviewTable;
