import { SortOptions } from "./types";

export const SIDEBAR_ITEMS_LIMIT = 1000;

export const SIDEBAR_SEARCH_THRESHOLD = {
  field: 20,
  value: 10,
} as const;

export const DEFAULT_FILTER_SIDEBAR_SORT: SortOptions = {
  by: "name",
  direction: "asc",
};
