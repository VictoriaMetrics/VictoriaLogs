import { useFilterSidebarVisible } from "./hooks/useFilterSidebarVisible";
import { SidebarCloseIcon, SidebarOpenIcon } from "../Main/Icons";
import Button from "../Main/Button/Button";

const FilterSidebarToggle = () => {
  const { isVisible, setVisible } = useFilterSidebarVisible();

  return (
    <Button
      variant="outlined"
      color={isVisible ? "gray" : "primary"}
      startIcon={isVisible ? <SidebarCloseIcon/> : <SidebarOpenIcon/>}
      onClick={() => setVisible(!isVisible)}
    >
      {isVisible ? "Hide" : "Show"} filters
    </Button>
  );
};

export default FilterSidebarToggle;
