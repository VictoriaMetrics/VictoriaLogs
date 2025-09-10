import { processNavigationItems } from "./utils";
import { getLogsNavigation } from "./navigation";
import { useAppState } from "../state/common/StateContext";

const useNavigationMenu = () => {
  const { appConfig } = useAppState();
  const showAlerting = appConfig?.vmalert?.enabled || false;
  const menu = getLogsNavigation({ showAlerting });
  return processNavigationItems(menu);
};

export default useNavigationMenu;
