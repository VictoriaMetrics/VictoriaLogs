import { FC, useEffect } from "preact/compat";
import Header from "../Header/Header";
import { matchPath, Outlet, useLocation } from "react-router-dom";
import "./style.scss";
import { getAppModeEnable } from "../../utils/app-mode";
import classNames from "classnames";
import Footer from "../Footer/Footer";
import { RouterOptions, routerOptions } from "../../router";
import useDeviceDetect from "../../hooks/useDeviceDetect";
import { footerLinksToLogs } from "../../constants/footerLinks";
import WebStorageCheck from "../../components/WebStorageCheck/WebStorageCheck";
import { migrateStorageToPrefixedKeys } from "../../utils/storage";
import { useAppState } from "../../state/common/StateContext";
import {
  useBrowserTabSync
} from "../../components/Configurators/GlobalSettings/BrowserTabController/hooks/useBrowserTabSync";
import useFetchAppConfig from "../../hooks/useFetchAppConfig";

const LogsLayout: FC = () => {
  const appModeEnable = getAppModeEnable();
  const { isMobile } = useDeviceDetect();
  const { pathname } = useLocation();
  const { isDarkTheme } = useAppState();
  useBrowserTabSync();
  // loads config.json into the app state - it carries the license type and the tenant aliases
  useFetchAppConfig();

  const setDocumentTitle = () => {
    const matchedEntry = Object.entries(routerOptions).find(([path]) => {
      return matchPath(path, pathname);
    });

    const routeTitle =  (matchedEntry?.[1] as RouterOptions)?.title;
    const defaultTitle = "UI for VictoriaLogs";
    document.title = routeTitle ? `${routeTitle} - ${defaultTitle}` : defaultTitle;
  };

  useEffect(setDocumentTitle, [pathname]);

  useEffect(() => {
    const migrateStorage = migrateStorageToPrefixedKeys();
    if (migrateStorage.removed.length || migrateStorage.migrated.length) {
      console.info(migrateStorage);
    }
  }, []);

  return <section
    className={classNames({
    "vm-container": true,
    "vm-container_dark": isDarkTheme
  })}
  >
    <Header/>
    <div
      id="vm-body"
      className={classNames({
        "vm-container-body": true,
        "vm-container-body_mobile": isMobile,
        "vm-container-body_app": appModeEnable
      })}
    >
      <Outlet/>
    </div>
    {!appModeEnable && <Footer links={footerLinksToLogs}/>}

    <WebStorageCheck/>
  </section>;
};

export default LogsLayout;
