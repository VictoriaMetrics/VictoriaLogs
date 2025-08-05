import { FC, useEffect } from "preact/compat";
import Header from "../Header/Header";
import { matchPath, Outlet, useLocation } from "react-router-dom";
import "./style.scss";
import { getAppModeEnable } from "../../utils/app-mode";
import classNames from "classnames";
import Footer from "../Footer/Footer";
import { RouterOptions, routerOptions } from "../../router";
import useDeviceDetect from "../../hooks/useDeviceDetect";
import ControlsLogsLayout from "./ControlsLogsLayout";
import { footerLinksToLogs } from "../../constants/footerLinks";

const LogsLayout: FC = () => {
  const appModeEnable = getAppModeEnable();
  const { isMobile } = useDeviceDetect();
  const { pathname } = useLocation();

  const setDocumentTitle = () => {
    const matchedEntry = Object.entries(routerOptions).find(([path]) => {
      return matchPath(path, pathname);
    });

    const routeTitle =  (matchedEntry?.[1] as RouterOptions)?.title;
    const defaultTitle = "UI for VictoriaLogs";
    document.title = routeTitle ? `${routeTitle} - ${defaultTitle}` : defaultTitle;
  };

  useEffect(setDocumentTitle, [pathname]);

  return <section className="vm-container">
    <Header controlsComponent={ControlsLogsLayout}/>
    <div
      className={classNames({
        "vm-container-body": true,
        "vm-container-body_mobile": isMobile,
        "vm-container-body_app": appModeEnable
      })}
    >
      <Outlet/>
    </div>
    {!appModeEnable && <Footer links={footerLinksToLogs}/>}
  </section>;
};

export default LogsLayout;
