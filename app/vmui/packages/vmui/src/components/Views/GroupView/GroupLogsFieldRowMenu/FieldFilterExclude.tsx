import { FC } from "preact/compat";
import { FilterOffIcon } from "../../../Main/Icons";
import { ExtraFilterOperator } from "../../../ExtraFilters/types";
import { useExtraFilters } from "../../../ExtraFilters/hooks/useExtraFilters";

interface Props {
  field: string;
  value: string;
  onClose: () => void;
}

const FieldFilterExclude: FC<Props> = ({ field, value, onClose }) => {
  const { extraFilters, addNewFilter, removeFilterByValue } = useExtraFilters();
  const filtersByValue = extraFilters.filter(f => f.field === field && f.value === value);
  const isExcludeFilter = filtersByValue.some(f => f.operator === ExtraFilterOperator.NotEquals);

  const handleClickFilter = () => {
    const newFilter = { field, value, operator: ExtraFilterOperator.NotEquals };
    if (isExcludeFilter) {
      // If the same filter already exists, we remove it by setting the value to an empty string
      removeFilterByValue(field, value);
    } else {
      addNewFilter(newFilter);
    }
    onClose();
  };

  return (
    <div
      className="vm-legend-hits-menu-row vm-legend-hits-menu-row_interactive"
      onClick={handleClickFilter}
    >
      <div className="vm-legend-hits-menu-row__icon">{<FilterOffIcon/>}</div>
      <div className="vm-legend-hits-menu-row__title">
        {isExcludeFilter ? "Remove exclude filter" : "Exclude this value"}
      </div>
    </div>
  );
};

export default FieldFilterExclude;
