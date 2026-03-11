import { FC, RefObject } from "preact/compat";
import Popper from "../../Main/Popper/Popper";
import { MenuAction } from "./FilterSidebarActions";

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
          {actions.map(({ id, icon, title, onClick }) => (
            <div
              className="vm-legend-hits-menu-row vm-legend-hits-menu-row_interactive"
              key={id}
              onClick={createHandlerClick(onClick, onClose)}
            >

              <div className="vm-legend-hits-menu-row__icon">{icon}</div>
              <div className="vm-legend-hits-menu-row__title">{title}</div>
            </div>
          ))}
        </div>
      </div>
    </Popper>
  );
};

export default FilterSidebarActionsMore;
