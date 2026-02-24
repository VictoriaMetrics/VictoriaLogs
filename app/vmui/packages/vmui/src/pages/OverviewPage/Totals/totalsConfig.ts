import { formatNumber } from "../../../utils/number";

export type TotalsConfig = {
  title: string;
  description: string;
  alias: string;
  stats: string;
  statsExpr: string;
  formatter?: (value: number) => string;
}

export const explorerTotals: TotalsConfig[] = [
  {
    title: "Total logs",
    description: "Total number of selected logs on the selected time range",
    alias: "totalLogs",
    stats: "count()",
    formatter: formatNumber,
  },
  {
    title: "Logs/sec (avg)",
    description: "Average logs per second on the selected time range",
    alias: "logsPerSec",
    stats: "rate()",
    formatter: formatNumber,
  },
  {
    title: "Unique log streams",
    description: "The number of log streams on the selected time range",
    alias: "uniqueStreams",
    stats: "count_uniq(_stream_id)",
    formatter: (n: number) => `${formatNumber(n)}`,
  },
].map(t => ({
  ...t,
  statsExpr: `${t.stats} as ${t.alias}`,
  description: t.description + `\n\`* | ${t.stats}\``,
}));
