export type QUERY_HISTORY = string[][];

export type QUERY_HISTORY_META = Record<string, number>

export type HistoryStorage = {
  QUERY_HISTORY?: string[][];
  QUERY_HISTORY_META?: QUERY_HISTORY_META;
  QUERY_FAVORITES?: string[][];
};

export type QueryHistoryEntry = {
  query: string;
  lastRunAt?: number;
};

export type QueryHistoryGroup = {
  title: string;
  entries: QueryHistoryEntry[];
};
