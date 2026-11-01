import { FetchLogsParams } from "../useFetchLogs";
import { FetchHitsParams, FetchHitsResult } from "../useFetchHits";
import { normalizeTimeParams } from "../../../../utils/time";
import { TimeParams } from "../../../../types";
import { BaseTriggers, HitsTriggers, LogsPhase, LogsTriggers, TriggersState } from "./types";
import { GRAPH_QUERY_MODE } from "../../../../components/Chart/BarHitsChart/types";

export const runWithTimeout = async <T>({ run, abort, isCurrent, timeoutMs }: {
  run: (startTimeout: () => void) => Promise<T>;
  abort: () => void;
  isCurrent: () => boolean;
  timeoutMs?: number;
}): Promise<{ result: T; didTimeout: boolean }> => {
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  let didTimeout = false;

  const startTimeout = () => {
    if (!timeoutMs || !isCurrent()) return;

    clearTimeout(timeoutId);

    timeoutId = setTimeout(() => {
      if (!isCurrent()) return;
      didTimeout = true;
      abort();
    }, timeoutMs);
  };

  try {
    const result = await run(startTimeout);
    return { result, didTimeout };
  } finally {
    clearTimeout(timeoutId);
  }
};

export const runHitsWithFallback = async ({ fetchHits, params, abort, isCurrent, timeoutMs }: {
  fetchHits: (params: FetchHitsParams) => Promise<FetchHitsResult>;
  params: FetchHitsParams;
  abort: () => void;
  isCurrent: () => boolean;
  timeoutMs?: number;
}): Promise<FetchHitsResult> => {
  const { result, didTimeout } = await runWithTimeout({
    run: startTimeout => {
      startTimeout();
      return fetchHits(params);
    },
    abort,
    isCurrent,
    timeoutMs: params.isIterative ? undefined : timeoutMs,
  });
  if (!isCurrent()) return "aborted";
  return didTimeout ? fetchHits({ ...params, isIterative: true }) : result;
};

export const getTriggerChanges = (prev: TriggersState, next: TriggersState) => ({
  base: prev.baseTriggers !== next.baseTriggers,
  logs: prev.logsTriggers !== next.logsTriggers,
  hits: prev.hitsTriggers !== next.hitsTriggers,
});

export const getHitsRunOptions = ({ prev, next, isFirstRun, pendingHits, timeoutMs }: {
  prev: TriggersState;
  next: TriggersState;
  isFirstRun: boolean;
  pendingHits: boolean;
  timeoutMs: number;
}) => {
  const changed = getTriggerChanges(prev, next);
  const { hitsTriggers } = next;
  const shouldRunHits = (isFirstRun || changed.base || changed.hits || pendingHits) && !hitsTriggers.isChartHidden;
  const forceHitsOnce = prev.hitsTriggers.executeHitsOnceTrigger !== hitsTriggers.executeHitsOnceTrigger;
  const isStatsMode = hitsTriggers.graphQueryMode === GRAPH_QUERY_MODE.stats;
  const allowIterative = !isStatsMode && !forceHitsOnce && timeoutMs > 0;

  return { shouldRunHits, allowIterative };
};

export const getRunOptions = ({ prev, next, isFirstRun, logsPhase, pendingHits, timeoutMs }: {
  prev: TriggersState;
  next: TriggersState;
  isFirstRun: boolean;
  logsPhase: LogsPhase;
  pendingHits: boolean;
  timeoutMs: number;
}) => {
  const changed = getTriggerChanges(prev, next);
  const shouldRunLogs = (isFirstRun || changed.base || changed.logs || logsPhase !== "idle") && !next.logsTriggers.isLogsHidden;
  const { shouldRunHits, allowIterative } = getHitsRunOptions({ prev, next, isFirstRun, pendingHits, timeoutMs });

  return {
    shouldRunLogs,
    shouldRunHits,
    shouldUseLogsTimeout: shouldRunHits && allowIterative,
  };
};

export const buildLogsParams = (
  baseTriggers: BaseTriggers,
  logsTriggers: LogsTriggers
): FetchLogsParams => ({
  query: baseTriggers.query,
  period: baseTriggers.period,
  extraParams: baseTriggers.extraParams,
  beforeFetch: logsTriggers.beforeFetch,
});

export const buildHitsParams = (
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

export const getSyncTimeFilterKey = ({ query, period }: BaseTriggers) => {
  return `${query}__${period.start}__${period.end}`;
};

export const isEqualPeriod = (prevPeriod: TimeParams, nextPeriod: TimeParams) => {
  const normalizeNext = normalizeTimeParams(nextPeriod);
  const isStartEqual = prevPeriod.start === normalizeNext.start;
  const isEndEqual = prevPeriod.end === normalizeNext.end;
  return isStartEqual && isEndEqual;
};
