import { FC, useMemo } from "preact/compat";
import { RouterOptions, routerOptions, RouterOptionsHeader } from "../../../router";
import { matchPath, useLocation } from "react-router-dom";
import "./style.scss";
import ControlsLogsLayout from "../../LogsLayout/ControlsLogsLayout";

export interface ControlsProps {
  displaySidebar: boolean;
  isMobile?: boolean;
  headerSetup?: RouterOptionsHeader;
}

const HeaderControls: FC<ControlsProps> = ({
  isMobile,
  ...props
}) => {
  const { pathname } = useLocation();

  const headerSetup = useMemo(() => {
    const matchedEntry = Object.entries(routerOptions).find(([path]) => {
      return matchPath(path, pathname);
    });

    return (matchedEntry?.[1] as RouterOptions)?.header || {};
  }, [pathname]);

  return (
    <ControlsLogsLayout
      {...props}
      isMobile={isMobile}
      headerSetup={headerSetup}
    />
  );
};

export default HeaderControls;
