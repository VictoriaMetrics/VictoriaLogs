import { ValuesMode } from "./TopFieldValues";
import { quoteFieldName } from "../../../../utils/regexp";

export const buildFieldValuesQuery = (
  field: string,
  mode: ValuesMode,
  limit: number,
) => {
  const qField = quoteFieldName(field);
  const where = `${qField}:*`;

  if (mode === "top") {
    return `${where} | top ${limit} by (${qField})`;
  }

  return [
    where,
    `| stats by (${qField}) count() as hits`,
    `| sort by (hits asc, ${qField})`,
    `| limit ${limit}`
  ].filter(Boolean).join(" ");
};
