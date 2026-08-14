import { createPortal, FC, RefObject } from "preact/compat";
import SelectLimit from "../../Main/Pagination/SelectLimit/SelectLimit";
import useSearchParamsFromObject from "../../../hooks/useSearchParamsFromObject";
import TableSettings, { TableSettingsProps } from "../../Table/TableSettings/TableSettings";

interface Props extends TableSettingsProps {
  rowsPerPage: number,
  targetRef: RefObject<HTMLElement>,
}

const TableLogsSettings: FC<Props> = ({ rowsPerPage, targetRef, ...settingsProps }) => {
  const { setSearchParamsFromKeys } = useSearchParamsFromObject();

  const handleSetRowsPerPage = (limit: number) => {
    setSearchParamsFromKeys({ rows_per_page: limit || "all" });
  };

  const controls = (
    <div className="vm-table-view-settings">
      <div className="vm-table-view-settings__button">
        <SelectLimit
          allowUnlimited
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
