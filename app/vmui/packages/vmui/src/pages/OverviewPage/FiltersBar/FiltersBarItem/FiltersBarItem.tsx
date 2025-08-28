import { FC } from "preact/compat";
import { ExtraFilter } from "../types";
import Button from "../../../../components/Main/Button/Button";
import { CloseIcon } from "../../../../components/Main/Icons";
import classNames from "classnames";
import "../style.scss";

type Props = {
  filter: ExtraFilter;
  isFocusable?: boolean;
  onRemove: () => void;
}

const FiltersBarItem: FC<Props> = ({ filter, isFocusable, onRemove }) => {
  return (
    <div
      className={classNames({
        "vm-filters-bar-item": true,
        "vm-filters-bar-item_focusable": isFocusable,
      })}
    >
      {filter.field} {filter.operator} {filter.value}

      <div
        className="vm-filters-bar-item-actions"
        onClick={(e) => e.stopPropagation()}
      >
        <Button
          size="small"
          color="gray"
          variant="text"
          startIcon={<CloseIcon/>}
          onClick={onRemove}
        />
      </div>
    </div>
  );
};

export default FiltersBarItem;
