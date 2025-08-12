import { useCallback, useMemo } from "preact/compat";
import { useSearchParams } from "react-router-dom";
import { ExtraFilter, ExtraFilterOperator } from "../types";
import { getNewExtraFilter } from "../utils";

const TOKENS = ["eq", "neq", "regex", "nregex"] as const;
type Token = typeof TOKENS[number];
const isToken = (x: unknown): x is Token =>
  typeof x === "string" && (TOKENS as readonly string[]).includes(x);

const tokenToOperator: Record<Token, ExtraFilterOperator> = {
  eq: ExtraFilterOperator.Equals,
  neq: ExtraFilterOperator.NotEquals,
  regex: ExtraFilterOperator.Regex,
  nregex: ExtraFilterOperator.NotRegex,
};
const operatorToToken: Record<ExtraFilterOperator, Token> = {
  [ExtraFilterOperator.Equals]: "eq",
  [ExtraFilterOperator.NotEquals]: "neq",
  [ExtraFilterOperator.Regex]: "regex",
  [ExtraFilterOperator.NotRegex]: "nregex",
};

export const useExtraFilters = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const extraFilters: ExtraFilter[] = useMemo(() => {
    return searchParams.getAll("filter").flatMap((param, id) => {
      try {
        const obj = JSON.parse(param);
        if (!obj || typeof obj !== "object") return [];
        const { f, o, v } = obj as Record<string, unknown>;
        if (typeof f !== "string" || typeof v !== "string" || !isToken(o)) return [];
        return [{ id, field: f, operator: tokenToOperator[o], value: v }];
      } catch {
        return [];
      }
    });
  }, [searchParams]);

  const extraParams = useMemo(() => {
    const params = new URLSearchParams();
    extraFilters.map(({ field, operator, value }) => {
      if (!field || !value || !operator) return;
      params.append(
        field === "_stream" ? "extra_stream_filters" : "extra_filters",
        `${field}:${operator}"${value}"`
      );
    });

    return params;
  }, [extraFilters]);

  const setNewFilters = useCallback((filters: ExtraFilter[]) => {
    const next = new URLSearchParams(searchParams);
    next.delete("filter");
    for (const f of filters) {
      next.append(
        "filter",
        JSON.stringify({ f: f.field, o: operatorToToken[f.operator], v: f.value })
      );
    }
    setSearchParams(next, { replace: true });
  }, [searchParams, setSearchParams]);

  const addNewFilter = useCallback(() => {
    const newFilter = getNewExtraFilter();
    setNewFilters([...extraFilters, newFilter]);
  }, [extraFilters, setNewFilters]);

  const updateFilter = useCallback((filter: ExtraFilter, index: number) => {
    const next = [...extraFilters];
    next[index] = filter;
    setNewFilters(next);
  }, [extraFilters, setNewFilters]);

  const removeFilter = useCallback((index: number) => {
    const next = extraFilters.filter((_f, i) => i !== index);
    setNewFilters(next);
  }, [extraFilters, setNewFilters]);

  return {
    extraFilters,
    extraParams,
    addNewFilter,
    updateFilter,
    removeFilter,
  };
};
