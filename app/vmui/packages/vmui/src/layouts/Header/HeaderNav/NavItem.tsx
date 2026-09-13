import { FC } from "preact/compat";
import { NavLink, useSearchParams } from "react-router-dom";
import { getTenantSearchParams } from "../../../hooks/useTenant";
import classNames from "classnames";
import { NavigationItemType } from "../../../router/navigation";

interface NavItemProps {
  activeMenu: string,
  label: string,
  value: string,
  type: NavigationItemType,
  color?: string,
}

const NavItem: FC<NavItemProps> = ({
  activeMenu,
  label,
  value,
  type,
  color
}) => {
  const [searchParams] = useSearchParams();
  const tenantSearch = getTenantSearchParams(searchParams);

  if (type === NavigationItemType.externalLink) return (
    <a
      className={classNames({
        "vm-header-nav-item": true,
        "vm-header-nav-item_active": activeMenu === value
      })}
      style={{ color }}
      href={value}
      target={"_blank"}
      rel="noreferrer"
    >
      {label}
    </a>
  );
  return (
    <NavLink
      className={classNames({
        "vm-header-nav-item": true,
        "vm-header-nav-item_active": activeMenu === value // || m.submenu?.find(m => m.value === activeMenu)
      })}
      style={{ color }}
      to={{ pathname: value, search: tenantSearch.toString() }}
    >
      {label}
    </NavLink>
  );
};

export default NavItem;
