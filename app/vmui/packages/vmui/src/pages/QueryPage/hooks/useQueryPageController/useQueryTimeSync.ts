import { useRef } from "preact/compat";
import { useFetchQueryTime } from "../useFetchQueryTime";
import { useQueryDispatch } from "../../../../state/query/QueryStateContext";
import { timeParamsToDateRange } from "../../../../utils/time";
import type { BaseTriggers } from "./types";
import { getSyncTimeFilterKey, isEqualPeriod } from "./utils";

export const useQueryTimeSync = () => {
  const { fetchQueryTime, abort } = useFetchQueryTime();
  const queryDispatch = useQueryDispatch();
  const lastSyncedTimeFilterKeyRef = useRef("");
  const pendingTimeFilterRef = useRef(false);

  const syncTimeFilter = async ({ baseTriggers, baseChanged, isCurrent }: {
    baseTriggers: BaseTriggers;
    baseChanged: boolean;
    isCurrent: () => boolean;
  }): Promise<boolean> => {
    const timeFilterKeyChanged = getSyncTimeFilterKey(baseTriggers) !== lastSyncedTimeFilterKeyRef.current;
    if (!timeFilterKeyChanged) {
      pendingTimeFilterRef.current = false;
      return true;
    }

    const shouldSync = baseChanged || pendingTimeFilterRef.current;

    if (!shouldSync) return true;

    pendingTimeFilterRef.current = true;
    const apiPeriod = await fetchQueryTime({
      query: baseTriggers.query,
      period: baseTriggers.period,
    });

    if (!isCurrent()) return false;

    const nextPeriod = apiPeriod ?? baseTriggers.period;
    const hasTimeFilter = Boolean(apiPeriod?.hasTimeFilter);

    queryDispatch({ type: "SET_QUERY_HAS_TIME_FILTER", payload: hasTimeFilter });
    lastSyncedTimeFilterKeyRef.current = getSyncTimeFilterKey({ ...baseTriggers, period: nextPeriod });
    pendingTimeFilterRef.current = false;

    if (hasTimeFilter && !isEqualPeriod(baseTriggers.period, nextPeriod)) {
      baseTriggers.setPeriod(
        { nextPeriod: timeParamsToDateRange(nextPeriod) },
        { replace: true }
      );
      return false; // Wait for the next sync with the updated period.
    }

    return true;
  };

  const resetTimeSync = () => {
    // The next base change should resolve the time filter again.
    lastSyncedTimeFilterKeyRef.current = "";
  };

  return { syncTimeFilter, resetTimeSync, abort };
};
