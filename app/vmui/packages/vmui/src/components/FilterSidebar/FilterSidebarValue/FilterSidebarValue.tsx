import { FC, useMemo } from "preact/compat";
import { LogsFieldValues } from "../../../api/types";
import { formatNumberShort } from "../../../utils/number";


import "./style.scss";
import classNames from "classnames";
import { ExtraFilter, ExtraFilterOperator } from "../../ExtraFilters/types";
import Checkbox from "../../Main/Checkbox/Checkbox";
import { useAppState } from "../../../state/common/StateContext";
import { DisabledIcon } from "../../Main/Icons";

type Props = {
  field: LogsFieldValues;
  fieldName: string;
  extraFilters: ExtraFilter[];
  isAnyValueFilter: boolean;
  onAddFilter: (filter: ExtraFilter) => void;
  onRemoveByValue: (field: string, value: string) => void;
}

const FilterSidebarValue: FC<Props> = ({
  field,
  fieldName,
  isAnyValueFilter,
  extraFilters,
  onAddFilter,
  onRemoveByValue,
}) => {
  const { isDarkTheme } = useAppState();

  const filtersByValue = useMemo(() => {
    return extraFilters.filter(f => f.value === field.value);
  }, [field.value, extraFilters]);

  const hasFilter = !!filtersByValue.length;
  const isExcluded = filtersByValue.some(f => f.operator === ExtraFilterOperator.NotEquals);

  const hitsShort = formatNumberShort(field.hits);

  const handleToggleFilter = () => {
    if (hasFilter) {
      onRemoveByValue(fieldName, field.value);
    } else {
      onAddFilter({
        field: fieldName,
        value: field.value,
        operator: ExtraFilterOperator.Equals,
        isStream: true,
      });
    }
  };

  return (
    <div
      className={classNames({
        "vm-filter-sidebar-value": true,
        "vm-filter-sidebar-value_active": hasFilter,
        "vm-filter-sidebar-value_empty": !field.hits
      })}
      onClick={handleToggleFilter}
    >
      <div className="vm-filter-sidebar-value__checkbox">
        <Checkbox
          size="small"
          checked={hasFilter || isAnyValueFilter}
          icon={isExcluded ? <DisabledIcon/> : undefined}
          color={hasFilter ? (isDarkTheme ? "secondary" : "primary") : "gray"}
        />
      </div>

      <div className="vm-filter-sidebar-value__title">
        {field.value}
        <span className="vm-filter-sidebar-value__hits">{" "}({hitsShort})</span>
      </div>
    </div>
  );
};

export default FilterSidebarValue;
