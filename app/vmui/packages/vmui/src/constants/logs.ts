import { DATE_TIME_FORMAT } from "./date";
import { getIsMobile } from "../hooks/useDeviceDetect";

export const LOGS_DOCS_URL = "https://docs.victoriametrics.com/victorialogs";

export const LOGS_DEFAULT_LIMIT = 50;
export const LOGS_CONFIRM_THRESHOLD = 1000;
export const LOGS_MAX_LIMIT = 10000;

export const LOGS_LIMIT_WARN_DISMISSED_KEY = "vmui.logs.limit.warn.dismissed";

export const LOGS_LIMIT_HITS = 5;

export const LOGS_BAR_COUNT_DEFAULT = getIsMobile() ? 24 : 96;

export const WITHOUT_GROUPING = "none";

// Default values for the logs configurators.
export const LOGS_GROUP_BY = "_stream";
export const LOGS_DISPLAY_FIELDS = "_msg";
export const LOGS_DATE_FORMAT = `${DATE_TIME_FORMAT}.SSS`;

// URL parameters for the logs page.
export const LOGS_URL_PARAMS = {
  LIMIT: "limit",
  GROUP_BY: "groupBy",
  DISPLAY_FIELDS: "displayFields",
  NO_WRAP_LINES: "noWrapLines",
  COMPACT_GROUP_HEADER: "compactGroupHeader",
  ROWS_PER_PAGE: "rows_per_page",
  COLUMNS: "columns",
};

// Maximum values for the logs autocomplete.
export const MAX_QUERY_FIELDS = 1;
export const MAX_QUERIES_HISTORY = 25;

// Default fields for the table.
export const DEFAULT_COMMON_FIELDS = ["_time", "_msg" ];
export const DEFAULT_STREAM_FIELDS = ["_stream"];
