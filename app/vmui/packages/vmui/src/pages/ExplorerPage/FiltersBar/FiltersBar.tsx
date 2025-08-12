import { FC, useEffect } from "preact/compat";
import "./style.scss";
import { EditIcon, EditOffIcon, FilterIcon, PlusIcon } from "../../../components/Main/Icons";
import { useExtraFilters } from "./hooks/useExtraFilters";
import Button from "../../../components/Main/Button/Button";
import FiltersBarItem from "./FiltersBarItem/FiltersBarItem";
import { ExtraFilter } from "./types";
import { useFilterViewMode } from "./hooks/useFilterViewMode";
import usePrevious from "../../../hooks/usePrevious";
import classNames from "classnames";

const FiltersBar: FC = () => {
  const { extraFilters, addNewFilter, updateFilter, removeFilter } = useExtraFilters();
  const { isEditMode, setIsEditMode, toggleEditMode } = useFilterViewMode();

  const extraFiltersLength = extraFilters.length;
  const extraFiltersLengthPrev = usePrevious(extraFiltersLength) || 0;

  useEffect(() => {
    // Enable edit mode if a new filter is added and we are not already in edit mode
    const isAddedFilter = (extraFiltersLength - extraFiltersLengthPrev) === 1;
    if (isAddedFilter && !isEditMode) {
      setIsEditMode(true);
    }
  }, [extraFiltersLength, extraFiltersLengthPrev, isEditMode, setIsEditMode]);

  // TODO: Maybe edit view make as modal or popup?
  return (
    <div className="vm-filters-bar vm-block">
      <div className="vm-filters-bar-header">
        <div className="vm-filters-bar-header-title">
          <FilterIcon/>
          <h2 className="vm-title">Filters</h2>
        </div>

        <div className="vm-filters-bar-header__controls">
          {!!extraFilters.length && (
            <Button
              variant="outlined"
              startIcon={isEditMode ? <EditOffIcon/> : <EditIcon/>}
              onClick={toggleEditMode}
            >
              {isEditMode ? "Compact" : "Edit"}
            </Button>
          )}

          <Button
            startIcon={<PlusIcon/>}
            onClick={addNewFilter}
          >
           Add filter
          </Button>
        </div>
      </div>

      {extraFilters.length > 0 && (
        <div
          className={classNames({
            "vm-filters-bar-body": true,
            "vm-filters-bar-body_readonly": !isEditMode,
          })}
        >
          {extraFilters.map((filter, index) => (
            <FiltersBarItem
              key={`${filter.field}_${filter.value}_${index}`}
              defaultFilter={filter}
              isEditMode={isEditMode}
              onEditMode={toggleEditMode}
              onChange={(next: ExtraFilter) => updateFilter(next, index)}
              onRemove={() => removeFilter(index)}
            />
          ))}
        </div>
      )}
    </div>
  );
};

export default FiltersBar;
