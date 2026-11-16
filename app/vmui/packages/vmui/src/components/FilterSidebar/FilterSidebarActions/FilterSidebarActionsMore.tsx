import { FC, RefObject } from "preact/compat";
import Popper from "../../Main/Popper/Popper";
import { MenuAction } from "./FilterSidebarActions";
import classNames from "classnames";
import { DoneIcon } from "../../Main/Icons";

type Props = {
  actions: MenuAction[]
  isOpen: boolean;
  buttonRef: RefObject<HTMLDivElement>;
  onClose: () => void;
}

const FilterSidebarActionsMore: FC<Props> = ({ actions, isOpen, buttonRef, onClose }) => {
  const createHandlerClick = (action: () => void, closeMenu: () => void) => () => {
    action();
    closeMenu();
  };

  return (
    <Popper
      placement="bottom-right"
      open={isOpen}
      buttonRef={buttonRef}
      onClose={onClose}
    >
      <div className="vm-legend-hits-menu">
        <div className="vm-legend-hits-menu-section">
          {actions.map(({ id, icon, title, onClick, isActive }) => (
            <button
              type="button"
              className={classNames({
                "vm-legend-hits-menu-row": true,
                "vm-legend-hits-menu-row_interactive": true,
                "vm-legend-hits-menu-row_active": isActive,
              })}
              key={id}
              onClick={createHandlerClick(onClick, onClose)}
            >

              {icon && <div className="vm-legend-hits-menu-row__icon">{icon}</div>}

              <div className="vm-legend-hits-menu-row__title">{title}</div>

              {isActive && (
                <div className="vm-legend-hits-menu-row__icon vm-legend-hits-menu-row__icon_active">
                  <DoneIcon/>
                </div>
              )}
            </button>
          ))}
        </div>
      </div>
    </Popper>
  );
};

export default FilterSidebarActionsMore;
