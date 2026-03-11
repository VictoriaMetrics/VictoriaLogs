import { ExtraFilter } from "../types";
import { groupByMultipleKeys } from "../../../utils/array";
import { isStreamFilter } from "./isStreamFilter";
import { EXTRA_FILTERS_KEY, EXTRA_STREAM_FILTERS_KEY } from "../constants";
import { buildFilterExpr, buildStreamExpr } from "./buildExprFromExtraFilters";

export const buildExtraFilterParams = (extraFilters: ExtraFilter[]) => {
  const params = new URLSearchParams();
  const grouped = groupByMultipleKeys(extraFilters, ["field", "operator", "isStream"]);

  grouped.forEach(({ values: filters }) => {
    const { field, operator } = filters[0] || {};

    if (!field || !operator) return;

    const isStream = isStreamFilter(filters[0]);
    const key = isStream ? EXTRA_STREAM_FILTERS_KEY : EXTRA_FILTERS_KEY;
    const buildExpr = isStream ? buildStreamExpr : buildFilterExpr;

    const filterValue = buildExpr({ field, operator, filters });
    params.append(key, filterValue);
  });

  return params;
};
