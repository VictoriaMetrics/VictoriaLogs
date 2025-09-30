import { useCallback, useMemo } from "preact/compat";
import { useSearchParams } from "react-router-dom";
import { ExtraFilter, ExtraFilterOperator } from "../FiltersBar/types";
import { groupByMultipleKeys } from "../../../utils/array";
import { escapeDoubleQuotes } from "../../../utils/regexp";

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

const escapeQuotes = (str: string) => str.replace(/"/g, "\\\"");

export const filterToExpr = (filter: ExtraFilter) => {
  const { field, operator, value: rawValue } = filter;

  const isStream = field === "_stream";
  const isEmptyValue = rawValue === "\"\"";
  const isAllValue = rawValue === "*";

  const shouldWrap = !isStream && !isAllValue && !isEmptyValue;
  const value = shouldWrap ? `"${escapeQuotes(rawValue)}"` : rawValue;

  switch (operator) {
    case ExtraFilterOperator.Equals:
    case ExtraFilterOperator.NotEquals: {
      const base = (isAllValue || isStream) ? `${field}:${value}` : `${field}:=${value}`;
      return operator === ExtraFilterOperator.NotEquals ? `(NOT ${base})` : base;
    }
    case ExtraFilterOperator.Regex:
    case ExtraFilterOperator.NotRegex: {
      const base = `${field}:~${value}`;
      return operator === ExtraFilterOperator.NotRegex ? `(NOT ${base})` : base;
    }
    default:
      return "";
  }
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

    groupByMultipleKeys(extraFilters, ["field", "operator"]).forEach(({ values }) => {
      const { field, operator, value } = values[0];

      if (!field || !values.length || !operator) return;

      const key =  field === "_stream" ? "extra_stream_filters" : "extra_filters";

      if (values.length === 1) {
        params.append(key, filterToExpr({ field, operator, value }));
      } else {
        const escapeValues = values.map(v => `"${escapeDoubleQuotes(v.value)}"`);
        const base = `${field}:in(\n${escapeValues.join(",\n")}\n)`;
        params.append(key, operator === ExtraFilterOperator.NotEquals ? `(NOT ${base})` : base);
      }
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

  const addNewFilter = useCallback((newFilter: ExtraFilter) => {
    setNewFilters([...extraFilters, newFilter]);
  }, [extraFilters, setNewFilters]);

  const updateFilter = useCallback((filter: ExtraFilter, index: number) => {
    const next = [...extraFilters];
    next[index] = filter;
    setNewFilters(next);
  }, [extraFilters, setNewFilters]);

  const upsertFilters = useCallback((newExtraFilters: ExtraFilter[]) => {
    const byKey = new Map<string, ExtraFilter>();

    for (const f of extraFilters) {
      byKey.set(`${f.field}\u0000${f.value}`, f);
    }

    for (const nf of newExtraFilters) {
      byKey.set(`${nf.field}\u0000${nf.value}`, nf);
    }

    const next = Array.from(byKey.values());
    setNewFilters(next);
  }, [extraFilters, setNewFilters]);

  const removeFilter = useCallback((index: number) => {
    const next = extraFilters.filter((_f, i) => i !== index);
    setNewFilters(next);
  }, [extraFilters, setNewFilters]);

  const clearFilters = useCallback(() => setNewFilters([]), [setNewFilters]);

  return {
    extraFilters,
    extraParams,
    addNewFilter,
    updateFilter,
    upsertFilters,
    removeFilter,
    clearFilters,
  };
};
