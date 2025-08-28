import { createContext, FC, useContext, useMemo, useReducer, Dispatch } from "preact/compat";
import { Action, OverviewState, initialState, reducer } from "./reducer";
import { getQueryStringValue } from "../../utils/query-string";

type StateContextType = { state: OverviewState, dispatch: Dispatch<Action> };

export const OverviewStateContext = createContext<StateContextType>({} as StateContextType);

export const useOverviewState = (): OverviewState => useContext(OverviewStateContext).state;
export const useOverviewDispatch = (): Dispatch<Action> => useContext(OverviewStateContext).dispatch;

export const initialPrepopulatedState = Object.entries(initialState)
  .reduce((acc, [key, value]) => ({
    ...acc,
    [key]: getQueryStringValue(key) || value
  }), {}) as OverviewState;

export const OverviewStateProvider: FC = ({ children }) => {
  const [state, dispatch] = useReducer(reducer, initialPrepopulatedState);

  const contextValue = useMemo(() => {
    return { state, dispatch };
  }, [state, dispatch]);

  return <OverviewStateContext.Provider value={contextValue}>
    {children}
  </OverviewStateContext.Provider>;
};


