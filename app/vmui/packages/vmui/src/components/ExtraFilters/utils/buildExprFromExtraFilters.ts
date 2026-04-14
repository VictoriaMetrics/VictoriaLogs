import { ExtraFilter, ExtraFilterOperator } from "../types";
import { escapeForLogsQLString } from "../../../utils/regexp";
import { isStreamFilter } from "./isStreamFilter";

type ExprBuilder = (options: BuildExprOptions) => string;

type BuildExprOptions = {
  field: string;
  operator: ExtraFilterOperator;
  filters: ExtraFilter[];
};


enum FieldType {
  Field,
  Stream,
  Time
}

const buildFilterExpr: ExprBuilder = ({ field, operator, filters }: BuildExprOptions) => {
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

const buildStreamExpr: ExprBuilder = ({ field, operator, filters }: BuildExprOptions) => {
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

const buildTimeExpr: ExprBuilder = ({ field, operator, filters }: BuildExprOptions) => {
  if (filters.length === 0) return "";

  const isRegexOp = operator === ExtraFilterOperator.Regex || operator === ExtraFilterOperator.NotRegex;
  const isNegationOp = operator === ExtraFilterOperator.NotEquals || operator === ExtraFilterOperator.NotRegex;

  if (isRegexOp) {
    console.warn(`${operator} operator is not supported for _time field.`);
  }

  const values = filters.map(f => f.value.trim()).filter(Boolean);

  if (values.length === 0) return "";

  const parts = values.map(v => `${field}:${v}`);
  const expr = parts.join(" OR ");

  return isNegationOp ? `NOT (${expr})` : expr;
};

const getFieldType = (filter: ExtraFilter): FieldType => {
  if (filter.field === "_time") return FieldType.Time;
  if (isStreamFilter(filter)) return FieldType.Stream;
  return FieldType.Field;
};

export const getExprBuilder = (filter: ExtraFilter): ExprBuilder => {
  switch (getFieldType(filter)) {
    case FieldType.Time:
      return buildTimeExpr;
    case FieldType.Stream:
      return buildStreamExpr;
    case FieldType.Field:
    default:
      return buildFilterExpr;
  }
};

export const filterToExpr = (filter: ExtraFilter) => {
  const { field, operator } = filter;
  const buildExpr = getExprBuilder(filter);
  return buildExpr({ field, operator, filters: [filter] });
};
