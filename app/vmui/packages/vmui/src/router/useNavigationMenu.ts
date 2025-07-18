import { processNavigationItems } from "./utils";
import { getLogsNavigation } from "./navigation";

const useNavigationMenu = () => {
  const menu = getLogsNavigation();
  return processNavigationItems(menu);
};

export default useNavigationMenu;


