import { useQueryDispatch, useQueryState } from "../../../../state/query/QueryStateContext";
import { getUpdatedHistory } from "../../../QueryHistory/utils";
import { useCallback } from "react";

const QUERY_HISTORY_KEY = "LOGS_QUERY_HISTORY";

export const useQueryHistory = () => {
  const { queryHistory } = useQueryState();
  const queryDispatch = useQueryDispatch();

  const updateHistory = useCallback((nextQuery: string, inputIdx = 0) => {
    const currentEntry = queryHistory[inputIdx] || null;
    const nextHistory = getUpdatedHistory(nextQuery, currentEntry);

    queryDispatch({
      type: "SET_QUERY_HISTORY",
      payload: {
        key: QUERY_HISTORY_KEY,
        history: [nextHistory],
      }
    });
  }, [queryDispatch, queryHistory]);

  return {
    queryHistory,
    updateHistory,
  };
};
