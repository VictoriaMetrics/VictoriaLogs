import { useEffect, useRef, useState } from "preact/compat";
import { useFetchLogs } from "../useFetchLogs";
import type { FetchLogsParams, FetchLogsResult } from "../useFetchLogs";
import { useLimitController } from "../../../../components/Configurators/LogsLimitController/hooks/useLimitController";
import {
  useBaseTriggers,
  useHitsTriggers,
  useLogsTriggers,
} from "../useQueryPageTriggers/";
import { useFetchHits } from "../useFetchHits";
import type { FetchHitsParams, FetchHitsResult } from "../useFetchHits";
import { useDebounceCallback } from "../../../../hooks/useDebounceCallback";
import { useQueryTimeSync } from "./useQueryTimeSync";
import { addQueryToHistoryStorage } from "../../../../components/QueryHistory/utils";
import { useIncrementalTimeout } from "../../HitsPanel/hooks/useIncrementalTimeout";
import { LogsPhase, QueryControllerOptions, TriggersState } from "./types";
import {
  buildHitsParams,
  runWithTimeout,
  runHitsWithFallback,
  buildLogsParams,
  getHitsRunOptions,
  getRunOptions,
  getTriggerChanges
} from "./utils";

export const useQueryPageController = (props: QueryControllerOptions) => {
  const { incrementalTimeoutMs } = useIncrementalTimeout();

  const { limit } = useLimitController();
  const limitRef = useRef(limit);
  limitRef.current = limit;

  const { fetchLogs, resetLogs, ...logsRequestState } = useFetchLogs();
  const { fetchHits, resetHits, ...hitsRequestState } = useFetchHits();
  const { syncTimeFilter, resetTimeSync, abort: abortTimeSync } = useQueryTimeSync();

  const baseTriggers = useBaseTriggers(props);
  const logsTriggers = useLogsTriggers(props);
  const hitsTriggers = useHitsTriggers();
  const latestTriggersRef = useRef<TriggersState>({ baseTriggers, logsTriggers, hitsTriggers });
  latestTriggersRef.current = { baseTriggers, logsTriggers, hitsTriggers };

  const isFirstSync = useRef(true);

  const logsPhaseRef = useRef<LogsPhase>("idle");
  const retryLogsLimitRef = useRef<number | undefined>();
  const pendingHitsRef = useRef(false);
  const syncIdRef = useRef<symbol | null>(null);
  const [isWaitingForHits, setIsWaitingForHits] = useState(false);

  // Baseline for detecting trigger changes, including hits changes deferred until logs finish.
  // These triggers have been accepted for synchronization; their requests may still be pending.
  const acceptedTriggersRef = useRef<TriggersState>({
    baseTriggers,
    logsTriggers,
    hitsTriggers,
  });

  const cancelAll = () => {
    syncIdRef.current = null;
    pendingHitsRef.current = false;
    hitsRequestState.abort?.();
    logsRequestState.abort?.();
    abortTimeSync();
    logsPhaseRef.current = "idle";
    setIsWaitingForHits(false);
    debouncedSyncQueries.cancel();
  };

  const runLogs = async ({ params, timeoutMs, syncId }: {
    params: FetchLogsParams;
    timeoutMs?: number;
    syncId: symbol;
  }): Promise<FetchLogsResult["status"] | "timeout"> => {
    logsPhaseRef.current = "loading";
    logsRequestState.abort();

    const { result, didTimeout } = await runWithTimeout({
      run: onStart => fetchLogs({ ...params, onStart }),
      abort: logsRequestState.abort,
      isCurrent: () => syncId === syncIdRef.current && !latestTriggersRef.current.hitsTriggers.isChartHidden,
      timeoutMs,
    });

    if (syncId !== syncIdRef.current) return "aborted";

    retryLogsLimitRef.current = result.limit;
    logsPhaseRef.current = didTimeout ? "retryPending" : "idle";
    return didTimeout ? "timeout" : result.status;
  };

  const runHits = async ({ params, timeoutMs, syncId }: {
    params: FetchHitsParams;
    timeoutMs?: number;
    syncId: symbol;
  }): Promise<FetchHitsResult> => {
    pendingHitsRef.current = true;
    const result = await runHitsWithFallback({
      fetchHits,
      params,
      abort: hitsRequestState.abort,
      isCurrent: () => syncId === syncIdRef.current,
      timeoutMs,
    });

    if (syncId !== syncIdRef.current) return "aborted";

    pendingHitsRef.current = false;
    return result;
  };

  const syncQueries = async (next: TriggersState) => {
    const prev = acceptedTriggersRef.current;
    const isFirstRun = isFirstSync.current;

    const changed = getTriggerChanges(prev, next);

    if (!isFirstRun && !changed.base && !changed.logs) {
      if (!changed.hits) return;
      if (logsPhaseRef.current === "loading") {
        acceptedTriggersRef.current = next;
        return;
      }
    }

    if (isFirstRun || changed.base || changed.logs) {
      logsPhaseRef.current = "idle";
    }

    const { baseTriggers, logsTriggers } = next;
    const timeoutMs = incrementalTimeoutMs;
    const runOptions = getRunOptions({
      prev,
      next,
      isFirstRun,
      logsPhase: logsPhaseRef.current,
      pendingHits: pendingHitsRef.current,
      timeoutMs,
    });
    const { shouldRunLogs, shouldUseLogsTimeout } = runOptions;
    let { shouldRunHits } = runOptions;

    acceptedTriggersRef.current = next;

    if (isFirstRun) {
      isFirstSync.current = false;
    }

    const syncId = Symbol();
    syncIdRef.current = syncId;

    logsRequestState.abort();
    hitsRequestState.abort();
    abortTimeSync();

    pendingHitsRef.current = shouldRunHits;

    if (!shouldRunLogs) {
      logsPhaseRef.current = "idle";
    } else if (logsPhaseRef.current !== "retryPending") {
      logsPhaseRef.current = "preparing";
    }

    setIsWaitingForHits(false);

    const canContinue = await syncTimeFilter({
      baseTriggers,
      baseChanged: isFirstRun || changed.base,
      isCurrent: () => syncId === syncIdRef.current,
    });

    if (!canContinue || syncId !== syncIdRef.current) {
      return;
    }

    if (!shouldRunLogs && !shouldRunHits) {
      return;
    }

    resetTimeSync();

    addQueryToHistoryStorage(baseTriggers.query);

    const logsParams = buildLogsParams(baseTriggers, logsTriggers);

    if (shouldRunHits) resetHits();
    if (shouldRunLogs) resetLogs();

    let didLogsTimeout = logsPhaseRef.current === "retryPending";

    if (shouldRunLogs && !didLogsTimeout) {
      const result = await runLogs({
        params: { ...logsParams, limit: limitRef.current },
        timeoutMs: shouldUseLogsTimeout ? timeoutMs : undefined,
        syncId,
      });

      if (syncId !== syncIdRef.current) return;

      didLogsTimeout = result === "timeout";
    }

    const latest = latestTriggersRef.current;
    if (latest.baseTriggers !== baseTriggers || latest.logsTriggers !== logsTriggers) return;

    const hitsOptions = getHitsRunOptions({
      prev,
      next: latest,
      isFirstRun,
      pendingHits: shouldRunHits,
      timeoutMs,
    });

    if (!shouldRunHits && hitsOptions.shouldRunHits) resetHits();

    shouldRunHits = hitsOptions.shouldRunHits;
    const { allowIterative } = hitsOptions;
    pendingHitsRef.current = shouldRunHits;
    acceptedTriggersRef.current = latest;
    const hitsParams = buildHitsParams(baseTriggers, latest.hitsTriggers);

    setIsWaitingForHits(didLogsTimeout && shouldRunHits);

    if (shouldRunHits) {
      const isIterative = didLogsTimeout && allowIterative;
      const hitsResult = await runHits({
        params: { ...hitsParams, isIterative },
        timeoutMs: !isIterative && allowIterative ? timeoutMs : undefined,
        syncId,
      });

      if (syncId !== syncIdRef.current) return;

      if (hitsResult === "aborted") {
        setIsWaitingForHits(false);
        return;
      }
    }

    // Triggers may change while awaiting hits, before the debounced sync updates syncId.
    const beforeRetry = latestTriggersRef.current;
    if (
      beforeRetry.baseTriggers !== baseTriggers ||
      beforeRetry.logsTriggers !== logsTriggers ||
      beforeRetry.hitsTriggers !== latest.hitsTriggers
    ) {
      return;
    }

    if (didLogsTimeout && !latestTriggersRef.current.logsTriggers.isLogsHidden) {
      setIsWaitingForHits(false);

      await runLogs({
        params: {
          ...logsParams,
          limit: retryLogsLimitRef.current,
          beforeFetch: undefined,
        },
        syncId,
      });

      if (syncId !== syncIdRef.current) return;

      const afterRetry = latestTriggersRef.current;
      if (afterRetry.baseTriggers !== baseTriggers || afterRetry.logsTriggers !== logsTriggers) return;
      if (afterRetry.hitsTriggers === latest.hitsTriggers) return;

      const updatedHitsOptions = getHitsRunOptions({
        prev: latest,
        next: afterRetry,
        isFirstRun: false,
        pendingHits: false,
        timeoutMs,
      });
      acceptedTriggersRef.current = afterRetry;
      if (!updatedHitsOptions.shouldRunHits) return;

      await runHits({
        params: { ...buildHitsParams(baseTriggers, afterRetry.hitsTriggers), isIterative: false },
        timeoutMs: updatedHitsOptions.allowIterative ? timeoutMs : undefined,
        syncId,
      });
    }
  };

  const debouncedSyncQueries = useDebounceCallback(syncQueries, 300);

  useEffect(() => cancelAll, []);

  useEffect(() => {
    debouncedSyncQueries({
      baseTriggers,
      logsTriggers,
      hitsTriggers,
    });
  }, [baseTriggers, logsTriggers, hitsTriggers, debouncedSyncQueries]);

  return {
    cancelAll,
    logsRequestState: { ...logsRequestState, isPending: isWaitingForHits },
    hitsRequestState,
  };
};
