import { FC } from "preact/compat";
import SelectLimit from "../../../components/Main/Pagination/SelectLimit/SelectLimit";
import Button from "../../../components/Main/Button/Button";
import { SearchIcon } from "../../../components/Main/Icons";
import { ReactNode } from "react";

type Props = {
  title: ReactNode | string;
  rowsPerPage: number;
  enableSearch?: boolean;
  onToggleShowSearch: () => void;
  onChangeRowsPerPage: (rowsPerPage: number) => void;
  headerControls?: ReactNode;
}

const OverviewTableHeader: FC<Props> = ({
  title,
  rowsPerPage,
  enableSearch,
  onToggleShowSearch,
  onChangeRowsPerPage,
  headerControls
}) => {
  return (
    <div className="vm-top-fields-header">
      <h2 className="vm-title vm-top-fields-header__title">
        {title}
      </h2>

      <div className="vm-top-fields-header__configs">
        {enableSearch && (
          <Button
            startIcon={<SearchIcon/>}
            variant="text"
            size="small"
            onClick={onToggleShowSearch}
          >
            Search
          </Button>
        )}

        {headerControls}

        <SelectLimit
          limit={rowsPerPage}
          onChange={onChangeRowsPerPage}
        />
      </div>
    </div>
  );
};

export default OverviewTableHeader;
