import { useRef } from "preact/compat";
import Button from "../../../Main/Button/Button";
import {
  MoreIcon
} from "../../../Main/Icons";
import Popper from "../../../Main/Popper/Popper";
import useBoolean from "../../../../hooks/useBoolean";
import "./style.scss";
import { useColumnOptions } from "./hooks/useColumnOptions";
import { ColumnPrefs } from "../../hooks/useTableColumnPrefs";
import { type Column } from "../../types";

export interface TableHeaderMenuProps<T> {
  column: Column<T>;
  columnPrefs?: ColumnPrefs;
  onResize: (width: number) => void;
  onWrap: (value: boolean) => void;
  onHideCol: () => void;
}

const TableHeaderMenu = <T extends object>(props: TableHeaderMenuProps<T>) => {
  const {
    value: openContextMenu,
    setFalse: handleCloseContextMenu,
    toggle: handleToggleContextMenu
  } = useBoolean(false);

  const buttonRef = useRef<HTMLDivElement>(null);

  const { sections } = useColumnOptions(props);

  const handleClick = (e: MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    handleToggleContextMenu();
  };

  const createHandlerClickByMenu = (callback?: () => void) => () => {
    callback && callback();
    handleToggleContextMenu();
  };

  return (
    <div className="vm-table-header-menu">
      <div ref={buttonRef}>
        <Button
          startIcon={<MoreIcon/>}
          variant="text"
          size="small"
          onClick={handleClick}
        />
      </div>

      <Popper
        placement="bottom-right"
        open={openContextMenu}
        buttonRef={buttonRef}
        onClose={handleCloseContextMenu}
      >
        <div className="vm-legend-hits-menu">
          {sections.map((options, idx) => (
            <div
              className="vm-legend-hits-menu-section"
              key={idx}
            >
              {options.map((option, id) => (
                <div
                  className="vm-legend-hits-menu-row vm-legend-hits-menu-row_interactive"
                  key={`${idx}_${id}`}
                  onClick={createHandlerClickByMenu(option.onClick)}
                >

                  <div className="vm-legend-hits-menu-row__icon">{option.icon}</div>
                  <div className="vm-legend-hits-menu-row__title">{option.title}</div>
                </div>
              ))}
            </div>
          ))}
        </div>
      </Popper>
    </div>
  );
};

export default TableHeaderMenu;
