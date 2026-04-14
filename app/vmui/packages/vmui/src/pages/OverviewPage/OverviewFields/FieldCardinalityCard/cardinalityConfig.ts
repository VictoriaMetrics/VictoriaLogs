import { formatNumber, formatPercent } from "../../../../utils/number";

export type StatKey = "distinct" | "coverage" | "ratio" | "coverageOfTotal";

type Ctx = { field: string };

export type StatConfig = {
  key: StatKey;
  title: string;
  description: (ctx: Ctx) => string;
  format: (value: number | null, ctx?: Ctx) => string;
};

export const cardinalityConfig: StatConfig[] = [
  {
    key: "distinct",
    title: "Distinct",
    description: ({ field }) =>
      `Number of unique values of \`${field}\`.\n` +
      "Shows value variety (cardinality).\n" +
      `\`${field}:* | stats count_uniq(${field})\`.`,
    format: (v) => formatNumber(v || 0),
  },
  {
    key: "ratio",
    title: "Distinct ratio",
    description: ({ field }) =>
      `Share of unique values of \`${field}\`.\n` +
      "Helps spot high-cardinality fields.\n" +
      "`distinct / coverage × 100`",
    format: (v) => formatPercent(v),
  },
  {
    key: "coverage",
    title: "Coverage",
    description: ({ field }) =>
      `Total logs containing \`${field}\`.\n` +
      "Shows how often the field appears.\n" +
      `\`${field}:* | stats count()\`.`,
    format: (v) => formatNumber(v || 0),
  },
  {
    key: "coverageOfTotal",
    title: "Coverage %",
    description: ({ field }) =>
      `Percent of all logs with \`${field}\`.\n` +
      "Useful to compare across datasets.\n" +
      "`coverage / total × 100`",
    format: (v) => formatPercent(v),
  },
];
