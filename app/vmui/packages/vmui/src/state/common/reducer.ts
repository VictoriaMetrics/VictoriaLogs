import { getDefaultServer } from "../../utils/default-server-url";
import { getFromStorage, saveToStorage } from "../../utils/storage";
import { AppConfig, Theme } from "../../types";
import { isDarkTheme } from "../../utils/theme";
import { removeTrailingSlash } from "../../utils/url";

interface Tenant {
  accountID: string;
  projectID: string;
  disableTenantInfo: boolean;
}

export interface AppState {
  serverUrl: string;
  tenantId?: Tenant;
  theme: Theme;
  isDarkTheme: boolean | null;
  appConfig: AppConfig
}

export type Action =
  | { type: "SET_SERVER", payload: string }
  | { type: "SET_THEME", payload: Theme }
  | { type: "SET_TENANT_ID", payload: Tenant }
  | { type: "SET_APP_CONFIG", payload: AppConfig }
  | { type: "SET_DARK_THEME" }

export const initialState: AppState = {
  serverUrl: removeTrailingSlash(getDefaultServer()),
  tenantId: undefined,
  theme: (getFromStorage("THEME") || Theme.system) as Theme,
  isDarkTheme: null,
  appConfig: {}
};

export function reducer(state: AppState, action: Action): AppState {
  switch (action.type) {
    case "SET_SERVER":
      return {
        ...state,
        serverUrl: removeTrailingSlash(action.payload)
      };
    case "SET_TENANT_ID":
      return {
        ...state,
        tenantId: action.payload
      };
    case "SET_THEME":
      saveToStorage("THEME", action.payload);
      return {
        ...state,
        theme: action.payload,
      };
    case "SET_DARK_THEME":
      return {
        ...state,
        isDarkTheme: isDarkTheme(state.theme)
      };
    case "SET_APP_CONFIG":
      return {
        ...state,
        appConfig: action.payload
      };
    default:
      throw new Error();
  }
}
