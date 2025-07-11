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
 * VictoriaLogs navigation menu
 */
export const getLogsNavigation = (): NavigationItem[] => [
  {
    label: routerOptions[router.home].title,
    value: router.home,
  },
];
