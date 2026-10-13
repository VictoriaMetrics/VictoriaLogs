import { FC, ReactNode, RefObject, useRef } from "preact/compat";
import Button from "../../Main/Button/Button";
import {
  CloseIcon, MoreIcon,
  SortIcon,
  WidthIcon
} from "../../Main/Icons";
import Tooltip from "../../Main/Tooltip/Tooltip";
import useBoolean from "../../../hooks/useBoolean";
import "./style.scss";
import "../../Chart/BarHitsChart/LegendHitsMenu/style.scss";
import FilterSidebarActionsMore from "./FilterSidebarActionsMore";
import { SortOptions } from "../types";

export type MenuAction = {
  id: string;
  icon?: ReactNode;
  isActive?: boolean;
  title?: string;
  ref?: RefObject<HTMLDivElement>;
  visible?: boolean;
  onClick: () => void;
}

type Props = {
  sort: SortOptions;
  onChangeSort: (sort: SortOptions) => void;
  onResetWidth: () => void;
  onClose: () => void;
}

const FilterSidebarActions: FC<Props> = ({
  sort,
  onChangeSort,
  onResetWidth,
  onClose,
}) => {
  const {
    value: isOpenMoreMenu,
    toggle: onToggleMoreMenu,
    setFalse: onCloseMoreMenu,
  } = useBoolean(false);

  const {
    value: isOpenSortMenu,
    toggle: onToggleSortMenu,
    setFalse: onCloseSortMenu,
  } = useBoolean(false);


  const moreMenuRef = useRef<HTMLDivElement>(null);
  const sortMenuRef = useRef<HTMLDivElement>(null);

  const isVisible = (action: MenuAction) => action.visible !== false;

  // Reserved for future actions. Currently empty since we only have 3 actions.
  const moreMenuActions: MenuAction[] = [].filter(isVisible);

  const sortMenuActions: MenuAction[] = [
    {
      id: "hits-desc",
      title: "Hits: high to low",
      isActive: sort.by === "hits" && sort.direction === "desc",
      onClick: () => onChangeSort({ by: "hits", direction: "desc" }),
    },
    {
      id: "hits-asc",
      title: "Hits: low to high",
      isActive: sort.by === "hits" && sort.direction === "asc",
      onClick: () => onChangeSort({ by: "hits", direction: "asc" }),
    },
    {
      id: "name-asc",
      title: "Name: A to Z",
      isActive: sort.by === "name" && sort.direction === "asc",
      onClick: () => onChangeSort({ by: "name", direction: "asc" }),
    },
    {
      id: "name-desc",
      title: "Name: Z to A",
      isActive: sort.by === "name" && sort.direction === "desc",
      onClick: () => onChangeSort({ by: "name", direction: "desc" }),
    },
  ];

  const baseActions: MenuAction[] = [
    {
      id: "sort",
      icon: <SortIcon/>,
      ref: sortMenuRef,
      title: "Sort by",
      onClick: onToggleSortMenu,
    },
    {
      id: "reset-width",
      icon: <WidthIcon/>,
      title: "Reset width",
      onClick: onResetWidth,
    },
    {
      id: "more",
      icon: <MoreIcon/>,
      ref: moreMenuRef,
      onClick: onToggleMoreMenu,
      visible: !!moreMenuActions.length,
    },
    {
      id: "close",
      icon: <CloseIcon/>,
      onClick: onClose,
    },
  ].filter(isVisible);

  return (
    <div className="vm-filter-sidebar-actions">
      {baseActions.map(({ id, icon, title, onClick, ref }) => (
        <Tooltip
          key={id}
          title={title}
          disabled={!title}
        >
          <div ref={ref}>
            <Button
              variant="text"
              color="gray"
              size="small"
              onClick={onClick}
              startIcon={icon}
            />
          </div>
        </Tooltip>
      ))}

      <FilterSidebarActionsMore
        actions={moreMenuActions}
        isOpen={isOpenMoreMenu}
        buttonRef={moreMenuRef}
        onClose={onCloseMoreMenu}
      />

      <FilterSidebarActionsMore
        actions={sortMenuActions}
        isOpen={isOpenSortMenu}
        buttonRef={sortMenuRef}
        onClose={onCloseSortMenu}
      />
    </div>
  );
};

export default FilterSidebarActions;
