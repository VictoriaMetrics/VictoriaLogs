import { FC, useMemo, useRef } from "preact/compat";
import FilterSidebarActionsMore from "../FilterSidebarActions/FilterSidebarActionsMore";
import useBoolean from "../../../hooks/useBoolean";
import Button from "../../Main/Button/Button";
import { SortIcon } from "../../Main/Icons";
import { useFilterSidebarSort } from "../hooks/useFilterSidebarSort";
import { MenuAction } from "../FilterSidebarActions/FilterSidebarActions";
import "./style.scss";

const FilterSidebarSort: FC = () => {
  const { sort, setSort } = useFilterSidebarSort();

  const {
    value: isOpenOptions,
    toggle: onToggleOptions,
    setFalse: onCloseOptions,
  } = useBoolean(false);

  const sortMenuRef = useRef<HTMLDivElement>(null);

  const sortMenuActions: MenuAction[] = useMemo(() => [
    {
      id: "hits-desc",
      title: "Most hits",
      isActive: sort.by === "hits" && sort.direction === "desc",
      onClick: () => setSort({ by: "hits", direction: "desc" }),
    },
    {
      id: "hits-asc",
      title: "Fewest hits",
      isActive: sort.by === "hits" && sort.direction === "asc",
      onClick: () => setSort({ by: "hits", direction: "asc" }),
    },
    {
      id: "name-asc",
      title: "A to Z",
      isActive: sort.by === "name" && sort.direction === "asc",
      onClick: () => setSort({ by: "name", direction: "asc" }),
    },
    {
      id: "name-desc",
      title: "Z to A",
      isActive: sort.by === "name" && sort.direction === "desc",
      onClick: () => setSort({ by: "name", direction: "desc" }),
    },
  ], [sort, setSort]);

  const activeSortOption = sortMenuActions.find(action => action.isActive);

  return (
    <div className="vm-filter-sidebar-sort">
      <div ref={sortMenuRef}>
        <Button
          variant="outlined"
          color="gray"
          size="small"
          onClick={onToggleOptions}
          startIcon={<SortIcon/>}
        >
          {activeSortOption?.title}
        </Button>
      </div>
      <FilterSidebarActionsMore
        actions={sortMenuActions}
        isOpen={isOpenOptions}
        buttonRef={sortMenuRef}
        onClose={onCloseOptions}
      />
    </div>
  );
};

export default FilterSidebarSort;
