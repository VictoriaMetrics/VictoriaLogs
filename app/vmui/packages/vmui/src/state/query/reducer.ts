import { getFromStorage, saveToStorage } from "../../utils/storage";
import { HistoryKey, setQueriesToStorage } from "../../components/QueryHistory/utils";
import {
  QueryAutocompleteCache,
  QueryAutocompleteCacheItem
} from "../../components/Configurators/QueryEditor/QueryAutocompleteCache";
import { AutocompleteOptions } from "../../components/Main/Autocomplete/Autocomplete";
import { getOverrideValue } from "../../components/Configurators/GlobalSettings/QueryTimeOverride/QueryTimeOverride";

export interface QueryHistoryType {
  index: number;
  values: string[];
}

export interface QueryState {
  queryHistory: QueryHistoryType[];
  autocomplete: boolean;
  autocompleteQuick: boolean;
  autocompleteCache: QueryAutocompleteCache;
  metricsQLFunctions: AutocompleteOptions[];
  queryHasTimeFilter: boolean;
  executeQueryTrigger: number;
}

export type QueryAction =
  | { type: "SET_QUERY_HISTORY_BY_INDEX", payload: { value: QueryHistoryType, queryNumber: number } }
  | { type: "SET_QUERY_HISTORY", payload: { key: HistoryKey, history: QueryHistoryType[] } }
  | { type: "TOGGLE_AUTOCOMPLETE" }
  | { type: "SET_AUTOCOMPLETE_QUICK", payload: boolean }
  | { type: "SET_AUTOCOMPLETE_CACHE", payload: { key: QueryAutocompleteCacheItem, value: string[] } }
  | { type: "SET_QUERY_HAS_TIME_FILTER", payload: boolean }
  | { type: "RUN_QUERY"}

export const initialQueryState: QueryState = {
  queryHistory: [],
  autocomplete: getFromStorage("AUTOCOMPLETE") as boolean || false,
  autocompleteQuick: false,
  autocompleteCache: new QueryAutocompleteCache(),
  metricsQLFunctions: [],
  queryHasTimeFilter: false,
  executeQueryTrigger: 0,
};

export function reducer(state: QueryState, action: QueryAction): QueryState {
  switch (action.type) {
    case "SET_QUERY_HISTORY":
      setQueriesToStorage(action.payload.key, action.payload.history);
      return {
        ...state,
        queryHistory: action.payload.history
      };
    case "SET_QUERY_HISTORY_BY_INDEX":
      state.queryHistory.splice(action.payload.queryNumber, 1, action.payload.value);
      return {
        ...state,
        queryHistory: state.queryHistory
      };
    case "TOGGLE_AUTOCOMPLETE":
      saveToStorage("AUTOCOMPLETE", !state.autocomplete);
      return {
        ...state,
        autocomplete: !state.autocomplete
      };
    case "SET_AUTOCOMPLETE_QUICK":
      return {
        ...state,
        autocompleteQuick: action.payload
      };
    case "SET_AUTOCOMPLETE_CACHE": {
      state.autocompleteCache.put(action.payload.key, action.payload.value);
      return {
        ...state
      };
    }
    case "SET_QUERY_HAS_TIME_FILTER":
      return {
        ...state,
        queryHasTimeFilter: getOverrideValue() ? action.payload : false
      };
    case "RUN_QUERY":
      return {
        ...state,
        executeQueryTrigger: state.executeQueryTrigger + 1
      };
    default:
      throw new Error();
  }
}
