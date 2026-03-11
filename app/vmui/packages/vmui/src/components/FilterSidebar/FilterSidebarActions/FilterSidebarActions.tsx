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

export type MenuAction = {
  id: string;
  icon: ReactNode;
  title?: string;
  ref?: RefObject<HTMLDivElement>;
  visible?: boolean;
  onClick: () => void;
}

type Props = {
  onToggleSort: () => void;
  onResetWidth: () => void;
  onClose: () => void;
}

const FilterSidebarActions: FC<Props> = ({
  onToggleSort,
  onResetWidth,
  onClose,
}) => {
  const {
    value: isOpenMenu,
    toggle: onToggleMenu,
    setFalse: onCloseMenu,
  } = useBoolean(false);

  const menuRef = useRef<HTMLDivElement>(null);

  const isVisible = (action: MenuAction) => action.visible !== false;

  // Reserved for future actions. Currently empty since we only have 3 actions.
  const moreMenuActions: MenuAction[] = [].filter(isVisible);

  const baseActions: MenuAction[] = [
    {
      id: "sort",
      icon: <SortIcon />,
      title: "Sort direction",
      onClick: onToggleSort,
    },
    {
      id: "reset-width",
      icon: <WidthIcon />,
      title: "Reset width",
      onClick: onResetWidth,
    },
    {
      id: "more",
      icon: <MoreIcon />,
      ref: menuRef,
      onClick: onToggleMenu,
      visible: !!moreMenuActions.length,
    },
    {
      id: "close",
      icon: <CloseIcon />,
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
        isOpen={isOpenMenu}
        buttonRef={menuRef}
        onClose={onCloseMenu}
      />
    </div>
  );
};

export default FilterSidebarActions;
