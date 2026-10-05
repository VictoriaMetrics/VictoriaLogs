import { LogsFieldValues } from "../../api/types";

type ParamsKey = string;

export const FIELD_NAMES_CACHE_LIMIT = 10;

export interface OverviewState {
  totalLogs: number;
  fieldNames: LogsFieldValues[];
  fieldNamesParamsKey: ParamsKey | null;
  fieldNamesCache: Map<ParamsKey, LogsFieldValues[]>;
  streamsFieldNames: LogsFieldValues[];
  streamsFieldNamesParamsKey: ParamsKey | null;
}

export type Action =
  | { type: "SET_TOTAL_LOGS"; payload: number }
  | { type: "SET_FIELD_NAMES"; payload: { key: ParamsKey; rows: LogsFieldValues[] } }
  | { type: "CACHE_FIELD_NAMES"; payload: { key: ParamsKey; rows: LogsFieldValues[] } }
  | { type: "SET_STREAM_FIELD_NAMES"; payload: { key: ParamsKey; rows: LogsFieldValues[] } }

export const initialState: OverviewState = {
  totalLogs: 0,
  fieldNames: [],
  fieldNamesParamsKey: null,
  fieldNamesCache: new Map(),
  streamsFieldNames: [],
  streamsFieldNamesParamsKey: null,
};

const addToFieldNamesCache = (cache: OverviewState["fieldNamesCache"], key: ParamsKey, rows: LogsFieldValues[]) => {
  const fieldNamesCache = new Map(cache);
  fieldNamesCache.delete(key);
  fieldNamesCache.set(key, rows);

  while (fieldNamesCache.size > FIELD_NAMES_CACHE_LIMIT) {
    const oldestKey = fieldNamesCache.keys().next().value;
    if (oldestKey === undefined) break;
    fieldNamesCache.delete(oldestKey);
  }

  return fieldNamesCache;
};

export function reducer(state: OverviewState, action: Action): OverviewState {
  switch (action.type) {
    case "SET_TOTAL_LOGS":
      return { ...state, totalLogs: action.payload };

    case "SET_FIELD_NAMES":
      return {
        ...state,
        fieldNames: action.payload.rows,
        fieldNamesParamsKey: action.payload.key,
        fieldNamesCache: addToFieldNamesCache(state.fieldNamesCache, action.payload.key, action.payload.rows),
      };

    case "CACHE_FIELD_NAMES":
      return {
        ...state,
        fieldNamesCache: addToFieldNamesCache(state.fieldNamesCache, action.payload.key, action.payload.rows),
      };

    case "SET_STREAM_FIELD_NAMES":
      return {
        ...state,
        streamsFieldNames: action.payload.rows,
        streamsFieldNamesParamsKey: action.payload.key,
      };

    default:
      throw new Error("Unknown action");
  }
}
