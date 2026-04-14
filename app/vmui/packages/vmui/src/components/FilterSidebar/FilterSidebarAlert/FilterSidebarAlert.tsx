import { FC } from "preact/compat";
import { ErrorIcon, InfoIcon, WarningIcon } from "../../Main/Icons";
import { useCallback } from "react";
import classNames from "classnames";
import "./style.scss";

type Props = {
  title: string;
  message?: string | Error;
  variant: "info" | "warning" | "error";
  isVisible?: boolean;
}

const FilterSidebarAlert: FC<Props> = ({ title, message, variant = "info", isVisible = false }) => {

  const Icon = useCallback(() => {
    switch (variant) {
      case "warning":
        return <WarningIcon/>;
      case "error":
        return <ErrorIcon/>;
      default:
        return <InfoIcon/>;
    }
  }, [variant]);

  if (!isVisible) return null;

  return (
    <div
      className={classNames({
      "vm-filter-sidebar-alert": true,
      [`vm-filter-sidebar-alert_${variant}`]: true
    })}
    >
      <div className="vm-filter-sidebar-alert__icon"><Icon/></div>
      <div className="vm-filter-sidebar-alert__title">{title}</div>
      {message && <div className="vm-filter-sidebar-alert__message">{String(message)}</div>}
    </div>
  );
};

export default FilterSidebarAlert;
