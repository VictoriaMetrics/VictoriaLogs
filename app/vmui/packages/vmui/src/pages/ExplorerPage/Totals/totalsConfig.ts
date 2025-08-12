import { convertBytes } from "../../../utils/bytes";

export type TotalsConfig = {
  title: string;
  description: string;
  alias: string;
  stats: string;
  formatter?: (value: number) => string;
}

const defaultFormatNumber = (n: number) => n.toLocaleString("en-US");

export const explorerTotals: TotalsConfig[] = [
  {
    title: "Total logs",
    description: "Total number of log entries in the selected time range after filters. Matches the sum of points on the Hits chart for this window.",
    alias: "totalLogs",
    stats: "count()",
    formatter: defaultFormatNumber,
  },
  {
    title: "Logs/sec (avg)",
    description: "Average ingestion rate over the selected window. Calculated as Total logs divided by the window duration. Useful for load and quota monitoring.",
    alias: "logsPerSec",
    stats: "rate()",
    formatter: defaultFormatNumber,
  },
  {
    title: "Unique streams",
    description: "Approximate number of distinct `_stream` values in the selected window (hash-based). Helps detect high cardinality and new sources.",
    alias: "uniqueStreams",
    stats: "count_uniq_hash(_stream)",
    formatter: (n: number) => `≈${defaultFormatNumber(n)}`,
  },
  {
    title: "Log size",
    description: "Sum of `_msg` byte lengths in the selected window. Indicates payload heaviness (e.g., stack traces, large JSON). Not the on-disk size, but correlates with bandwidth/storage.",
    alias: "totalMsgBytes",
    stats: "sum_len(_msg)",
    formatter: (value: number) => convertBytes(value),
  }
];
