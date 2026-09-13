import { useEffect, useRef } from "preact/compat";
import { useHitsController } from "./useHitsController";
import { useLogsController } from "./useLogsController";
import {
  useBaseTriggers,
  useHitsTriggers,
  useLogsTriggers,
} from "../useQueryPageTriggers/";
import { BeforeFetch, FetchLogsParams } from "../useFetchLogs";
import { FetchHitsParams } from "../useFetchHits";
import { useDebounceCallback } from "../../../../hooks/useDebounceCallback";
import { useFetchQueryTime } from "../useFetchQueryTime";
import { useQueryDispatch } from "../../../../state/query/QueryStateContext";
import { TimeParams } from "../../../../types";
import { normalizeTimeParams, timeParamsToDateRange } from "../../../../utils/time";
import { addQueryToHistoryStorage } from "../../../../components/QueryHistory/utils";

export type UseQueryPageControllerProps = {
  query: string;
  beforeFetch?: BeforeFetch;
};

type BaseTriggers = ReturnType<typeof useBaseTriggers>;
type LogsTriggers = ReturnType<typeof useLogsTriggers>;
type HitsTriggers = ReturnType<typeof useHitsTriggers>;

type TriggersState = {
  baseTriggers: BaseTriggers;
  logsTriggers: LogsTriggers;
  hitsTriggers: HitsTriggers;
};

const getChanged = (prev: TriggersState, next: TriggersState) => ({
  base: prev.baseTriggers !== next.baseTriggers,
  logs: prev.logsTriggers !== next.logsTriggers,
  hits: prev.hitsTriggers !== next.hitsTriggers,
});

const buildLogsParams = (
  baseTriggers: BaseTriggers,
  logsTriggers: LogsTriggers
): FetchLogsParams => ({
  query: baseTriggers.query,
  period: baseTriggers.period,
  extraParams: baseTriggers.extraParams,
  beforeFetch: logsTriggers.beforeFetch,
});

const buildHitsParams = (
  baseTriggers: BaseTriggers,
  hitsTriggers: HitsTriggers
): FetchHitsParams => ({
  query: baseTriggers.query,
  period: baseTriggers.period,
  extraParams: baseTriggers.extraParams,
  step: hitsTriggers.step,
  field: hitsTriggers.groupFieldHits,
  fieldsLimit: hitsTriggers.topHits,
  queryMode: hitsTriggers.graphQueryMode,
});

const getSyncTimeFilterKey = ({ query, period }: BaseTriggers) => {
  return `${query}__${period.start}__${period.end}`;
};

const isEqualPeriod = (prevPeriod: TimeParams, nextPeriod: TimeParams) => {
  const normalizeNext = normalizeTimeParams(nextPeriod);
  const isStartEqual = prevPeriod.start === normalizeNext.start;
  const isEndEqual = prevPeriod.end === normalizeNext.end;
  return isStartEqual && isEndEqual;
};

export const useQueryPageController = (props: UseQueryPageControllerProps) => {
  const queryDispatch = useQueryDispatch();

  const { runLogs, ...logsRequestState } = useLogsController();
  const { runHits, ...hitsRequestState } = useHitsController();
  const { fetchQueryTime, ...timeRequestState } = useFetchQueryTime();

  const baseTriggers = useBaseTriggers(props);
  const logsTriggers = useLogsTriggers(props);
  const hitsTriggers = useHitsTriggers();

  const lastSyncedTimeFilterKeyRef = useRef("");
  const isFirstRender = useRef(true);

  const prevRef = useRef<TriggersState>({
    baseTriggers,
    logsTriggers,
    hitsTriggers,
  });

  const cancelAll = () => {
    hitsRequestState.abort?.();
    logsRequestState.abort?.();
    timeRequestState.abort?.();
  };

  const resolveTimeFilter = async (baseTriggersArg: BaseTriggers) => {
    const apiPeriod = await fetchQueryTime({
      query: baseTriggersArg.query,
      period: baseTriggersArg.period,
    });
    const nextPeriod: TimeParams = apiPeriod ?? baseTriggersArg.period;
    const hasTimeFilter = Boolean(apiPeriod?.hasTimeFilter);

    queryDispatch({ type: "SET_QUERY_HAS_TIME_FILTER", payload: hasTimeFilter });

    lastSyncedTimeFilterKeyRef.current = getSyncTimeFilterKey({ ...baseTriggersArg, period: nextPeriod });

    return {
      hasTimeFilter,
      nextPeriod,
    };
  };

  const syncQueries = async (next: TriggersState) => {
    const prev = prevRef.current;
    const isFirstRun = isFirstRender.current;

    const changed = getChanged(prev, next);

    const { baseTriggers, logsTriggers, hitsTriggers } = next;
    const { isLogsHidden } = logsTriggers;
    const { isChartHidden } = hitsTriggers;

    const shouldRunLogs = (isFirstRun || changed.base || changed.logs) && !isLogsHidden;
    const shouldRunHits = (isFirstRun || changed.base || changed.hits) && !isChartHidden;

    prevRef.current = next;
    if (isFirstRun) {
      isFirstRender.current = false;
    }

    const currentTimeFilterKey = getSyncTimeFilterKey(baseTriggers);
    const timeFilterKeyChanged = currentTimeFilterKey !== lastSyncedTimeFilterKeyRef.current;
    const shouldSyncTimeFilter = (isFirstRun || changed.base) && timeFilterKeyChanged;

    if (shouldSyncTimeFilter) {
      const { hasTimeFilter, nextPeriod } = await resolveTimeFilter(baseTriggers);
      const isSamePeriod = isEqualPeriod(baseTriggers.period, nextPeriod);

      if (hasTimeFilter && !isSamePeriod) {
        baseTriggers.setPeriod(
          { nextPeriod: timeParamsToDateRange(nextPeriod) },
          { replace: true }
        );

        return; // wait for next sync with updated period
      }
    }

    if (!shouldRunLogs && !shouldRunHits) {
      return;
    }

    // Reset time sync marker after successful main flow.
    // The next base change should run time sync again.
    lastSyncedTimeFilterKeyRef.current = "";

    addQueryToHistoryStorage(baseTriggers.query);

    const logsParams = buildLogsParams(baseTriggers, logsTriggers);
    const hitsParams = buildHitsParams(baseTriggers, hitsTriggers);

    if (shouldRunLogs) {
      const logsOk = await runLogs(logsParams);
      if (!logsOk) return;
    }

    if (shouldRunHits) {
      await runHits(hitsParams);
    }
  };

  const debouncedSyncQueries = useDebounceCallback(syncQueries, 300);

  useEffect(() => {
    debouncedSyncQueries({
      baseTriggers,
      logsTriggers,
      hitsTriggers,
    });
  }, [baseTriggers, logsTriggers, hitsTriggers, debouncedSyncQueries]);

  return {
    cancelAll,
    logsRequestState,
    hitsRequestState,
  };
};
