import { FC } from "preact/compat";
import { DeleteIcon } from "../../../components/Main/Icons";
import { useExtraFilters } from "../hooks/useExtraFilters";
import FiltersBarItem from "./FiltersBarItem/FiltersBarItem";
import Button from "../../../components/Main/Button/Button";
import "./style.scss";

const FiltersBar: FC = () => {
  const { extraFilters, removeFilter, clearFilters } = useExtraFilters();

  if (!extraFilters.length) return null;

  return (
    <div className="vm-filters-bar vm-block">
      <div className="vm-filters-bar-title">
        <h2 className="vm-title">Global filters:</h2>
      </div>

      {extraFilters.map((filter, index) => (
        <FiltersBarItem
          key={`${filter.field}_${filter.value}_${index}`}
          filter={filter}
          onRemove={() => removeFilter(index)}
        />
      ))}

      {!!extraFilters.length && (
        <div className="vm-filters-bar__actions">
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
