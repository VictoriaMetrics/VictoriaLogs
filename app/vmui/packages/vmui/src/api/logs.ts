export const getLogsUrl = (server: string): string =>
  `${server}/select/logsql/query`;

export const getLogHitsUrl = (server: string): string =>
  `${server}/select/logsql/hits`;

export const getStatsQueryRangeUrl = (server: string): string =>
  `${server}/select/logsql/stats_query_range`;
