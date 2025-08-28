import { ValuesMode } from "./TopFieldValues";

export const buildFieldValuesQuery = (
  field: string,
  mode: ValuesMode,
  limit: number,
) => {
  const where = `${field}:*`;

  if (mode === "top") {
    return `${where} | top ${limit} by (${field})`;
  }

  return [
    where,
    `| stats by (${field}) count() as hits`,
    `| sort by (hits asc, ${field})`,
    `| limit ${limit}`
  ].filter(Boolean).join(" ");
};
