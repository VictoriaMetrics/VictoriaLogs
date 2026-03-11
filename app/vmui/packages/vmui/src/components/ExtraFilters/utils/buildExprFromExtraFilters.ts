import { ExtraFilter, ExtraFilterOperator } from "../types";
import { escapeForLogsQLString } from "../../../utils/regexp";
import { isStreamFilter } from "./isStreamFilter";

type BuildExprOptions = {
  field: string;
  operator: ExtraFilterOperator;
  filters: ExtraFilter[];
};

export const buildFilterExpr = ({ field, operator, filters }: BuildExprOptions) => {
  const normalValues: string[] = [];
  const specialValues: string[] = [];
  if (filters.length === 0) return "";

  for (const f of filters) {
    if (f.value === "*" || f.value === "\"\"") {
      specialValues.push(f.value);
    } else {
      normalValues.push(`"${escapeForLogsQLString(f.value)}"`);
    }
  }

  const isRegexOp = operator === ExtraFilterOperator.Regex || operator === ExtraFilterOperator.NotRegex;
  const isNegationOp = operator === ExtraFilterOperator.NotEquals || operator === ExtraFilterOperator.NotRegex;

  let expr = "";

  if (normalValues.length) {
    if (isRegexOp) {
      // (field:~"v1" OR field:~"v2")
      expr = `(${normalValues.map(v => `${field}:~${v}`).join(" OR ")})`;
    } else {
      // field:in("v1","v2")
      expr = `${field}:in(${normalValues.join(", ")})`;
    }
  }

  if (specialValues.length) {
    if (isRegexOp) {
      console.warn(`${operator} operator is not supported for ${specialValues.join(" and ")} values.`);
    }

    const specialExpr = specialValues.map(v => `${field}:${v}`).join(" OR ");
    expr = (expr ? `${expr} OR ` : "") + specialExpr;
  }

  if (isNegationOp) {
    // NOT (field:in("v1","v2")) or NOT (field:~"v1" OR field:~"v2")
    expr = `NOT (${expr})`;
  }

  return expr.trim();
};

export const buildStreamExpr = ({ field, operator, filters }: BuildExprOptions) => {
  if (filters.length === 0) return "";

  const escapedValues = filters.map(f => escapeForLogsQLString(f.value));

  const isRegexOp = operator === ExtraFilterOperator.Regex || operator === ExtraFilterOperator.NotRegex;
  const isNegationOp = operator === ExtraFilterOperator.NotEquals || operator === ExtraFilterOperator.NotRegex;

  let expr: string;

  if (field === "_stream") {
    if (isRegexOp) {
      console.warn(`${operator} operator is not supported for _stream field.`);
    }

    expr = filters.map(f => f.value).join(" OR ");
    return isNegationOp ? `NOT (${expr})` : expr;
  }

  if (isRegexOp) {
    // field=~"v1|...|vN" or field!~"v1|...|vN"
    const operatorSymbol = isNegationOp ? "!~" : "=~";
    expr = `${field}${operatorSymbol}"${escapedValues.join("|")}"`;
  } else {
    // field in ("v1","v2") or field not_in ("v1","v2")
    const membershipOp = isNegationOp ? "not_in" : "in";
    expr = `${field} ${membershipOp} (${escapedValues.map(v => `"${v}"`).join(", ")})`;
  }

  return `{${expr.trim()}}`;
};

export const filterToExpr = (filter: ExtraFilter) => {
  const { field, operator } = filter;
  const isStream = isStreamFilter(filter);
  const buildExpr = isStream ? buildStreamExpr : buildFilterExpr;
  return buildExpr({ field, operator, filters: [filter] });
};
