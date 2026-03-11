import { useFilterSidebarVisible } from "./hooks/useFilterSidebarVisible";
import { FilterListIcon } from "../Main/Icons";
import Button from "../Main/Button/Button";

const FilterSidebarToggle = () => {
  const { isVisible, setVisible } = useFilterSidebarVisible();

  return (
    <Button
      variant="outlined"
      color={isVisible ? "primary" : "gray"}
      startIcon={<FilterListIcon/>}
      onClick={() => setVisible(!isVisible)}
    >
      Filters
    </Button>
  );
};

export default FilterSidebarToggle;
