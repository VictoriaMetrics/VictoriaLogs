import { FC, useEffect, useRef } from "preact/compat";
import { useLocation } from "react-router-dom";
import classNames from "classnames";
import HeaderNav from "../HeaderNav/HeaderNav";
import useClickOutside from "../../../hooks/useClickOutside";
import MenuBurger from "../../../components/Main/MenuBurger/MenuBurger";
import "./style.scss";
import useBoolean from "../../../hooks/useBoolean";

interface SidebarHeaderProps {
  background: string
  color: string
}

const SidebarHeader: FC<SidebarHeaderProps> = ({
  background,
  color,
}) => {
  const { pathname } = useLocation();

  const sidebarRef = useRef<HTMLDivElement>(null);

  const {
    value: openMenu,
    toggle: handleToggleMenu,
    setFalse: handleCloseMenu,
  } = useBoolean(false);

  useEffect(handleCloseMenu, [pathname]);

  useClickOutside(sidebarRef, handleCloseMenu);

  return <div
    className="vm-header-sidebar"
    ref={sidebarRef}
  >
    <div
      className={classNames({
        "vm-header-sidebar-button": true,
        "vm-header-sidebar-button_open": openMenu
      })}
      onClick={handleToggleMenu}
    >
      <MenuBurger open={openMenu}/>
    </div>
    <div
      className={classNames({
        "vm-header-sidebar-menu": true,
        "vm-header-sidebar-menu_open": openMenu
      })}
    >
      <div>
        <HeaderNav
          color={color}
          background={background}
          direction="column"
        />
      </div>
    </div>
  </div>;
};

export default SidebarHeader;
