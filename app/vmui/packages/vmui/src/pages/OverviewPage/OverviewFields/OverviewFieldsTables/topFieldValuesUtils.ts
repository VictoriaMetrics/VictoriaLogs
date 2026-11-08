import { ValuesMode } from "./TopFieldValues";
import { quoteLogsQLFieldName } from "../../../../utils/logs";

export const buildFieldValuesQuery = (
  field: string,
  mode: ValuesMode,
  limit: number,
) => {
  const name = quoteLogsQLFieldName(field);
  const where = `${name}:*`;

  if (mode === "top") {
    return `${where} | top ${limit} by (${name})`;
  }

  return [
    where,
    `| stats by (${name}) count() as hits`,
    `| sort by (hits asc, ${name})`,
    `| limit ${limit}`
  ].filter(Boolean).join(" ");
};
