import { MetricBase } from "../api/types";

export const getNameForMetric = (result: MetricBase, alias?: string, showQueryNum = true): string => {
  const { __name__, ...freeFormFields } = result.metric;
  const queryPrefix = showQueryNum ? `[Query ${result.group}] ` : "";

  if (alias) {
    return getLabelAlias(result.metric, alias);
  }

  const name = `${queryPrefix}${__name__ || ""}`;

  if (Object.keys(freeFormFields).length === 0) {
    return name || "value";
  }

  const fieldsString = Object.entries(freeFormFields)
    .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
    .join(", ");

  return `${name}{${fieldsString}}`;
};

export const getLabelAlias = (fields: { [p: string]: string }, alias: string) => {
  return alias.replace(/\{\{(\w+)}}/g, (_, key) => fields[key] || "");
};

export const promValueToNumber = (s: string): number => {
  // See https://prometheus.io/docs/prometheus/latest/querying/api/#expression-query-result-formats
  switch (s) {
    case "NaN":
      return NaN;
    case "Inf":
    case "+Inf":
      return Infinity;
    case "-Inf":
      return -Infinity;
    default:
      return parseFloat(s);
  }
};

export const buildMetricLabel = (metric: Record<string, string>): string => {
  if (!metric) return "";

  const { __name__, ...rest } = metric;
  const name = __name__ ?? "";

  const labels = Object.entries(rest)
    .filter(([_k, v]) => v)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}:"${v}"`)
    .join(", ");

  if (!name && !labels) return "";

  return labels ? `${name} {${labels}}` : name;
};
