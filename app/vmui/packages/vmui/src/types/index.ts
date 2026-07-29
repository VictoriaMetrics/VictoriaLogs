import { Logs } from "../api/types";

export * from "./uplot";

declare global {
  interface Window {
    __VMUI_PREDEFINED_DASHBOARDS__: string[];
  }
}

export interface TimeParams {
  /** Timestamp in nanoseconds. */
  start: bigint;
  /** Timestamp in nanoseconds. */
  end: bigint;
}

export interface TimePeriod {
  /** ISO 8601 string with up to nanosecond precision, e.g. `"2026-06-01T12:00:24.414146743Z"` */
  from: string;
  /** ISO 8601 string with up to nanosecond precision, e.g. `"2026-06-01T12:00:24.414146743Z"` */
  to: string;
}

export enum ErrorTypes {
  emptyServer = "Please enter Server URL",
  validServer = "Please provide a valid Server URL",
  validQuery = "Please enter a valid Query and execute it",
}

export interface RelativeTimeOption {
  id: string,
  duration: string,
  /** Returns ISO 8601 string with up to nanosecond precision, e.g. `"2026-06-01T12:00:24.414146743Z"` */
  until: () => string,
  title: string,
  isDefault?: boolean,
}

export interface Timezone {
  region: string,
  utc: string,
  search?: string
}

export enum Theme {
  system = "system",
  light = "light",
  dark = "dark",
}

export interface AppConfig {
  license?: {
    type?: "enterprise" | "opensource";
  }
}

export interface GroupLogsType {
  keys: string[]
  keysString: string
  values: Logs[]
  pairs: string[]
  total: number
}

export type OrderDir = "asc" | "desc";

export interface NavigateOptions {
  /** Replace the current entry in the history stack instead of pushing a new one */
  replace?: boolean;
}

export type IntervalOption = {
  duration: string,
  valueNs: bigint
}
