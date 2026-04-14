import { LogsFieldValues } from "../../api/types";

type ParamsKey = string;

export interface OverviewState {
  totalLogs: number;
  fieldNames: LogsFieldValues[];
  fieldNamesParamsKey: ParamsKey | null;
  streamsFieldNames: LogsFieldValues[];
  streamsFieldNamesParamsKey: ParamsKey | null;
}

export type Action =
  | { type: "SET_TOTAL_LOGS"; payload: number }
  | { type: "SET_FIELD_NAMES"; payload: { key: ParamsKey; rows: LogsFieldValues[] } }
  | { type: "SET_STREAM_FIELD_NAMES"; payload: { key: ParamsKey; rows: LogsFieldValues[] } }

export const initialState: OverviewState = {
  totalLogs: 0,
  fieldNames: [],
  fieldNamesParamsKey: null,
  streamsFieldNames: [],
  streamsFieldNamesParamsKey: null,
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
