import { FC, ReactNode, RefObject, useRef } from "preact/compat";
import Button from "../../Main/Button/Button";
import { CloseIcon, MoreIcon, WidthIcon } from "../../Main/Icons";
import Tooltip from "../../Main/Tooltip/Tooltip";
import useBoolean from "../../../hooks/useBoolean";
import "./style.scss";
import "../../Chart/BarHitsChart/LegendHitsMenu/style.scss";
import FilterSidebarActionsMore from "./FilterSidebarActionsMore";

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
  onResetWidth: () => void;
  onClose: () => void;
}

const FilterSidebarActions: FC<Props> = ({
  onResetWidth,
  onClose,
}) => {
  const {
    value: isOpenMoreMenu,
    toggle: onToggleMoreMenu,
    setFalse: onCloseMoreMenu,
  } = useBoolean(false);

  const moreMenuRef = useRef<HTMLDivElement>(null);
  const isVisible = (action: MenuAction) => action.visible !== false;

  // Reserved for future actions. Currently empty since we only have 3 actions.
  const moreMenuActions: MenuAction[] = [].filter(isVisible);

  const baseActions: MenuAction[] = [
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
    </div>
  );
};

export default FilterSidebarActions;
