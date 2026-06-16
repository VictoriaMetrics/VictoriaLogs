import { setTimezone, initTimezone } from "../../utils/time";
import { saveToStorage } from "../../utils/storage";

export interface TimeState {
  timezone: string;
  defaultTimezone?: string;
}

export type TimeAction =
  | { type: "SET_TIMEZONE", payload: string }
  | { type: "SET_DEFAULT_TIMEZONE", payload: string }

export const initialTimeState: TimeState = {
  timezone: initTimezone(),
};

export function reducer(state: TimeState, action: TimeAction): TimeState {
  switch (action.type) {
    case "SET_TIMEZONE":
      setTimezone(action.payload);
      saveToStorage("TIMEZONE", action.payload);
      if (state.defaultTimezone) saveToStorage("DISABLED_DEFAULT_TIMEZONE", action.payload !== state.defaultTimezone);
      return {
        ...state,
        timezone: action.payload
      };
    case "SET_DEFAULT_TIMEZONE":
      return {
        ...state,
        defaultTimezone: action.payload
      };
    default:
      throw new Error();
  }
}
