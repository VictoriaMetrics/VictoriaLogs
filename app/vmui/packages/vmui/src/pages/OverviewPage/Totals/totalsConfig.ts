export type TotalsConfig = {
  title: string;
  description: string;
  alias: string;
  stats: string;
  statsExpr: string;
  formatter?: (value: number) => string;
}

const defaultFormatNumber = (n: number) => n.toLocaleString("en-US");

export const explorerTotals: TotalsConfig[] = [
  {
    title: "Total logs",
    description: "Total number of selected logs on the selected time range",
    alias: "totalLogs",
    stats: "count()",
    formatter: defaultFormatNumber,
  },
  {
    title: "Logs/sec (avg)",
    description: "Average logs per second on the selected time range",
    alias: "logsPerSec",
    stats: "rate()",
    formatter: defaultFormatNumber,
  },
  {
    title: "Unique log streams",
    description: "The number of log streams on the selected time range",
    alias: "uniqueStreams",
    stats: "count_uniq(_stream_id)",
    formatter: (n: number) => `${defaultFormatNumber(n)}`,
  },
].map(t => ({
  ...t,
  statsExpr: `${t.stats} as ${t.alias}`,
  description: t.description + `\n\`* | ${t.stats}\``,
}));
