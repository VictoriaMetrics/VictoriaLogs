import { FC } from "preact/compat";
import Button from "../../../Main/Button/Button";
import { FilterIcon } from "../../../Main/Icons";
import Tooltip from "../../../Main/Tooltip/Tooltip";
import { ExtraFilterOperator } from "../../../ExtraFilters/types";
import { useExtraFilters } from "../../../ExtraFilters/hooks/useExtraFilters";

interface Props {
  field: string;
  value: string;
  isStreamField: boolean;
}

const FieldFilterInclude: FC<Props> = ({ field, value, isStreamField }) => {
  const { extraFilters, addNewFilter, removeFilterByValue } = useExtraFilters();
  const filtersByValue = extraFilters.filter(f => f.field === field && f.value === value);
  const isIncludeFilter = filtersByValue.some(f => f.operator === ExtraFilterOperator.Equals);

  const handleClickFilter = () => {
    const newFilter = { field, value, operator: ExtraFilterOperator.Equals, isStream: isStreamField };
    if (isIncludeFilter) {
      // If the same filter already exists, we remove it by setting the value to an empty string
      removeFilterByValue(field, value);
    } else {
      addNewFilter(newFilter);
    }
  };

  return (
    <Tooltip title={isIncludeFilter ? "Remove include filter" : "Filter by this value"}>
      <Button
        variant="text"
        color={isIncludeFilter ? "secondary" : "gray"}
        size="small"
        startIcon={<FilterIcon/>}
        onClick={handleClickFilter}
      />
    </Tooltip>
  );
};

export default FieldFilterInclude;
