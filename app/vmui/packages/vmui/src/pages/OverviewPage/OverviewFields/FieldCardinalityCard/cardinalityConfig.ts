export type StatKey = "distinct" | "coverage" | "ratio" | "coverageOfTotal";

type Ctx = { field: string };

export type StatConfig = {
  key: StatKey;
  title: string;
  description: (ctx: Ctx) => string;
  format: (value: number | null, ctx?: Ctx) => string;
};

const fmtInt = (n: number) => n.toLocaleString("en-US");
const fmtPct = (p: number | null) => {
  if (p == null) return "-";
  if (p >= 1) return p.toFixed(1) + "%";
  if (p >= 0.01) return p.toFixed(2) + "%";
  return "<0.01%";
};

export const cardinalityConfig: StatConfig[] = [
  {
    key: "distinct",
    title: "Distinct",
    description: ({ field }) =>
      `Number of unique values of \`${field}\`.\n` +
      "Shows value variety (cardinality).\n" +
      `\`${field}:* | stats count_uniq(${field})\`.`,
    format: (v) => fmtInt((v as number) || 0),
  },
  {
    key: "ratio",
    title: "Distinct ratio",
    description: ({ field }) =>
      `Share of unique values of \`${field}\`.\n` +
      "Helps spot high-cardinality fields.\n" +
      "`distinct / coverage × 100`",
    format: (v) => fmtPct(v),
  },
  {
    key: "coverage",
    title: "Coverage",
    description: ({ field }) =>
      `Total logs containing \`${field}\`.\n` +
      "Shows how often the field appears.\n" +
      `\`${field}:* | stats count()\`.`,
    format: (v) => fmtInt((v as number) || 0),
  },
  {
    key: "coverageOfTotal",
    title: "Coverage %",
    description: ({ field }) =>
      `Percent of all logs with \`${field}\`.\n` +
      "Useful to compare across datasets.\n" +
      "`coverage / total × 100`",
    format: (v) => fmtPct(v),
  },
];
