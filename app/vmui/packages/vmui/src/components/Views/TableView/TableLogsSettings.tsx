import { createPortal, FC, RefObject } from "preact/compat";
import SelectLimit from "../../Main/Pagination/SelectLimit/SelectLimit";
import useSearchParamsFromObject from "../../../hooks/useSearchParamsFromObject";
import TableSettings, { TableSettingsProps } from "../../Table/TableSettings/TableSettings";

interface Props extends TableSettingsProps {
  rowsPerPage: number,
  setRowsPerPage: (limit: number) => void,
  targetRef: RefObject<HTMLElement>,
}

const TableLogsSettings: FC<Props> = ({ rowsPerPage, setRowsPerPage, targetRef, ...settingsProps }) => {
  const { setSearchParamsFromKeys } = useSearchParamsFromObject();

  const handleSetRowsPerPage = (limit: number) => {
    setRowsPerPage(limit);
    setSearchParamsFromKeys({ rows_per_page: limit });
  };

  const controls = (
    <div className="vm-table-view-settings">
      <div className="vm-table-view-settings__button">
        <SelectLimit
          limit={rowsPerPage}
          onChange={handleSetRowsPerPage}
        />
      </div>
      <div className="vm-table-view__settings-buttons">
        <TableSettings {...settingsProps}/>
      </div>
    </div>
  );

  if (!targetRef.current) return null;

  return createPortal(controls, targetRef.current);
};

export default TableLogsSettings;
