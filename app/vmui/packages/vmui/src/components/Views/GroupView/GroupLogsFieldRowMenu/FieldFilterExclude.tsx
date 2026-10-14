import { FC } from "preact/compat";
import { FilterOffIcon } from "../../../Main/Icons";
import { ExtraFilterOperator } from "../../../ExtraFilters/types";
import { useExtraFilters } from "../../../ExtraFilters/hooks/useExtraFilters";
import Tooltip from "../../../Main/Tooltip/Tooltip";
import Button from "../../../Main/Button/Button";

interface Props {
  field: string;
  value: string;
  isStreamField: boolean;
}

const FieldFilterExclude: FC<Props> = ({ field, value, isStreamField }) => {
  const { extraFilters, addNewFilter, removeFilterByValue } = useExtraFilters();
  const filtersByValue = extraFilters.filter(f => f.field === field && f.value === value);
  const isExcludeFilter = filtersByValue.some(f => f.operator === ExtraFilterOperator.NotEquals);

  const handleClickFilter = () => {
    const newFilter = { field, value, operator: ExtraFilterOperator.NotEquals, isStream: isStreamField };
    if (isExcludeFilter) {
      // If the same filter already exists, we remove it by setting the value to an empty string
      removeFilterByValue(field, value);
    } else {
      addNewFilter(newFilter);
    }
  };

  return (
    <Tooltip title={isExcludeFilter ? "Remove exclude filter" : "Exclude this value"}>
      <Button
        variant="text"
        color={isExcludeFilter ? "secondary" : "gray"}
        size="small"
        startIcon={<FilterOffIcon/>}
        onClick={handleClickFilter}
        aria-label={isExcludeFilter ? "Remove exclude filter" : "Exclude this value"}
      />
    </Tooltip>
  );
};

export default FieldFilterExclude;
