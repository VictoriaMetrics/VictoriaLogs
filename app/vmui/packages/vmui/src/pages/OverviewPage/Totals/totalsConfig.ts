import { convertBytes } from "../../../utils/bytes";

export type TotalsConfig = {
  title: string;
  description: string;
  alias: string;
  stats: string;
  statsExpr: string;
  approx?: boolean;
  formatter?: (value: number) => string;
}

const defaultFormatNumber = (n: number) => n.toLocaleString("en-US");

export const explorerTotals: TotalsConfig[] = [
  {
    title: "Total logs",
    description:
      "Total number of log entries.\n" +
      "Shows overall log volume.",
    alias: "totalLogs",
    stats: "count()",
    formatter: defaultFormatNumber,
  },
  {
    title: "Logs/sec (avg)",
    description:
      "Average logs per second.\n" +
      "Useful for monitoring ingestion rate.",
    alias: "logsPerSec",
    stats: "rate()",
    formatter: defaultFormatNumber,
  },
  {
    title: "Log size",
    description:
      "Sum of `_msg` byte lengths.\n" +
      "Highlights heavy payloads.",
    alias: "totalMsgBytes",
    stats: "sum_len(_msg)",
    formatter: (value: number) => (value ? convertBytes(value) : "0 KiB"),
  },
  {
    title: "Unique streams",
    description:
      "Number of distinct `_stream` values (approx).\n" +
      "Helps detect new sources and cardinality.",
    alias: "uniqueStreams",
    stats: "count_uniq_hash(_stream)",
    approx: true,
    formatter: (n: number) => `${defaultFormatNumber(n)}`,
  },
].map(t => ({
  ...t,
  statsExpr: `${t.stats} as ${t.alias}`,
  description: t.description + `\n\`* | ${t.stats}\``,
}));
