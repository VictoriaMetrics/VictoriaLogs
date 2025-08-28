import { FC } from "preact/compat";
import { DeleteIcon, FilterIcon } from "../../../components/Main/Icons";
import { useExtraFilters } from "../hooks/useExtraFilters";
import FiltersBarItem from "./FiltersBarItem/FiltersBarItem";
import Button from "../../../components/Main/Button/Button";
import "./style.scss";
import { useFieldFilter, useStreamFieldFilter } from "../hooks/useFieldFilter";
import Tooltip from "../../../components/Main/Tooltip/Tooltip";
import { ExtraFilterOperator } from "./types";

const FiltersBar: FC = () => {
  const { extraFilters, removeFilter, clearFilters } = useExtraFilters();
  const { fieldFilter, fieldValueFilter, setFieldFilter } = useFieldFilter();
  const { streamFieldFilter, streamFieldValueFilter, setStreamFieldFilter } = useStreamFieldFilter();

  if (!extraFilters.length && !fieldFilter && !streamFieldFilter) return null;

  return (
    <div className="vm-filters-bar vm-block">
      <div className="vm-filters-bar-title">
        <FilterIcon/>
        <h2 className="vm-title">Global filters:</h2>
      </div>

      {extraFilters.map((filter, index) => (
        <FiltersBarItem
          key={`${filter.field}_${filter.value}_${index}`}
          filter={filter}
          onRemove={() => removeFilter(index)}
        />
      ))}

      {fieldFilter && (
        <Tooltip title={"Focus - preview only. Doesn’t change Global filters."}>
          <FiltersBarItem
            isFocusable
            key={fieldFilter}
            filter={{ field: fieldFilter, value: fieldValueFilter || "*", operator: ExtraFilterOperator.Equals }}
            onRemove={() => setFieldFilter("")}
          />
        </Tooltip>
      )}

      {streamFieldFilter && (
        <Tooltip title={"Stream focus - preview only. Doesn’t change Global filters."}>
          <FiltersBarItem
            isFocusable
            key={fieldFilter}
            filter={{ field: streamFieldFilter, value: streamFieldValueFilter || "*", operator: ExtraFilterOperator.Equals }}
            onRemove={() => setStreamFieldFilter("")}
          />
        </Tooltip>
      )}

      {!!extraFilters.length && (
        <div className="vm-filters-bar__clear">
          <Button
            variant="text"
            color="error"
            size={"small"}
            onClick={clearFilters}
            startIcon={<DeleteIcon/>}
          >
            Clear global filters
          </Button>
        </div>
      )}
    </div>
  );
};

export default FiltersBar;
