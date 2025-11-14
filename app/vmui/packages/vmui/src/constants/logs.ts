import { DATE_TIME_FORMAT } from "./date";

export const LOGS_DEFAULT_LIMIT = 50;
export const LOGS_CONFIRM_THRESHOLD = 1000;
export const LOGS_MAX_LIMIT = 10000;

export const LOGS_LIMIT_WARN_DISMISSED_KEY = "vmui.logs.limit.warn.dismissed";

export const LOGS_BARS_VIEW = 100;
export const LOGS_LIMIT_HITS = 5;

// "Ungrouped" is a string that is used as a value for the "groupBy" parameter.
export const WITHOUT_GROUPING = "Ungrouped";

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
  DATE_FORMAT: "dateFormat",
  ROWS_PER_PAGE: "rows_per_page",
};

// Maximum values for the logs autocomplete.
export const MAX_QUERY_FIELDS = 1;
export const MAX_QUERIES_HISTORY = 25;

