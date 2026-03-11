import { ExtraFilter } from "../types";
import { isExtraFilterToken, extraFilterTokenToOperator } from "./operatorToken";
import { EXTRA_STREAM_FILTERS_KEY } from "../constants";

export const parseExtraFiltersFromSearchParams = (
  searchParams: URLSearchParams,
  searchParamKey: string
): ExtraFilter[] => {
  return searchParams.getAll(searchParamKey).flatMap((param, id) => {
    try {
      const obj = JSON.parse(param);
      if (!obj || typeof obj !== "object") return [];
      const { f, o, v } = obj as Record<string, unknown>;

      if (typeof f !== "string" || typeof v !== "string" || !isExtraFilterToken(o)) return [];

      return [{
        id,
        field: f,
        operator: extraFilterTokenToOperator[o],
        value: v,
        isStream: searchParamKey === EXTRA_STREAM_FILTERS_KEY,
      }];
    } catch {
      return [];
    }
  });
};
