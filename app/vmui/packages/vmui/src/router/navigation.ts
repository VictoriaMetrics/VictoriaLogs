import router, { routerOptions } from "./index";

export enum NavigationItemType {
  internalLink,
  externalLink,
}

export interface NavigationItem {
  label?: string,
  value?: string,
  hide?: boolean
  submenu?: NavigationItem[],
  type?: NavigationItemType,
}

/**
 * Submenu for Alerting tab
 */

const getAlertingNav = () => [
  { value: router.rules },
  { value: router.notifiers },
];

interface NavigationConfig {
  showAlerting: boolean,
}

/**
 * VictoriaLogs navigation menu
 */
export const getLogsNavigation = ({
  showAlerting,
}: NavigationConfig): NavigationItem[] => [
  {
    label: routerOptions[router.home].title,
    value: router.home,
  },
  {
    value: "Alerting",
    submenu: getAlertingNav(),
    hide: !showAlerting,
  },
];
