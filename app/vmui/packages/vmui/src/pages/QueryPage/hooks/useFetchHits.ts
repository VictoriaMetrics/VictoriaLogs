import { useCallback, useEffect, useRef, useState } from "preact/compat";
import { ErrorTypes, TimeParams } from "../../../types";
import { LogHits } from "../../../api/types";
import { useTenant } from "../../../hooks/useTenant";
import { useHideChart } from "../HitsPanel/hooks/useHideChart";
import { useAppState } from "../../../state/common/StateContext";
import { GRAPH_QUERY_MODE } from "../../../components/Chart/BarHitsChart/types";
import { buildHitsRequestHeaders, buildHitsRequestParams } from "../utils/buildHitsRequest";
import { fetchHitsStats } from "../utils/fetchHitsStats";
import { fetchHitsOnce } from "../utils/fetchHitsOnce";
import { fetchHitsIterative } from "../utils/fetchHitsIterative";
import { mergeLogHits } from "../utils/mergeLogHits";
import { useIncrementalTimeout } from "../HitsPanel/hooks/useIncrementalTimeout";

export interface FetchHitsParams {
  query: string;
  period: TimeParams;
  extraParams?: URLSearchParams;
  field?: string;
  fieldsLimit?: number;
  step: string | null;
  queryMode?: GRAPH_QUERY_MODE
  allowIterative?: boolean;
}

interface OptionsParams extends FetchHitsParams {
  signal: AbortSignal;
}

export const useFetchHits = () => {
  const { serverUrl } = useAppState();
  const tenant = useTenant();
  const [hideChart] = useHideChart();
  const { incrementalTimeoutMs } = useIncrementalTimeout();

  const [logHits, setLogHits] = useState<LogHits[]>([]);
  const [isLoading, setIsLoading] = useState<{ [key: number]: boolean; }>([]);
  const [error, setError] = useState<ErrorTypes | string>();
  const [durationMs, setDurationMs] = useState<number | undefined>();
  const abortControllerRef = useRef(new AbortController());

  const isIterative = useRef(false);

  const getOptions = ({ signal, ...params }: OptionsParams) => {
    return {
      signal,
      method: "POST",
      body: buildHitsRequestParams(params),
      headers: buildHitsRequestHeaders({ tenant }),
    };
  };

  const handleUpdateIterative = (nextHits: LogHits[], durationMs?: number) => {
    setLogHits(prev => mergeLogHits(prev, nextHits));
    setDurationMs(prev => (prev ?? 0) + (durationMs ?? 0));
  };

  const handleUpdateLoadingHits = (loadingHit?: LogHits) => {
    setLogHits(prev => {
      const nextHits = prev.filter(h => !h._isLoading);
      if (loadingHit) nextHits.unshift(loadingHit);
      return nextHits;
    });
  };

  const fetchHits = useCallback(async (params: FetchHitsParams) => {
    const queryMode = params.queryMode || GRAPH_QUERY_MODE.hits;
    const isStatsMode = queryMode === GRAPH_QUERY_MODE.stats;

    abortControllerRef.current.abort();

    const loadController = new AbortController();
    const firstRequestController = new AbortController();
    const firstSignal = AbortSignal.any([
      firstRequestController.signal,
      loadController.signal,
    ]);

    abortControllerRef.current = loadController;

    if (!params.step) {
      console.warn("Missing step; using fallback interval", params.period);
    }

    const id = Date.now();
    setIsLoading(prev => ({ ...prev, [id]: true }));

    let timeoutId: number | undefined = undefined;

    if (!isStatsMode && (params.allowIterative ?? true) && incrementalTimeoutMs) {
      timeoutId = window.setTimeout(() => {
        firstRequestController.abort();
      }, incrementalTimeoutMs);
    }

    const fetchFunc = isStatsMode ? fetchHitsStats : fetchHitsOnce;

    isIterative.current = false;
    setLogHits([]);
    setDurationMs(undefined);
    setError(undefined);

    try {
      const options = getOptions({ ...params, signal: firstSignal });
      const init = { ...options, url: serverUrl };

      try {
        const { hits, durationMs } = await fetchFunc(init);

        if (loadController.signal.aborted) return;

        isIterative.current = false;
        setDurationMs(durationMs);
        setLogHits(hits);
        return true;
      } catch (error) {
        if (loadController.signal.aborted) return;
        const isAbortError = error instanceof Error && error.name === "AbortError";
        if (!firstRequestController.signal.aborted || !isAbortError) {
          // noinspection ExceptionCaughtLocallyJS
          throw error;
        }
      }

      init.signal = loadController.signal;
      isIterative.current = true;

      await fetchHitsIterative({
        ...init,
        onUpdateLoading: handleUpdateLoadingHits,
        onUpdate: (hits, durationMs) => {
          if (loadController.signal.aborted) return;
          handleUpdateIterative(hits, durationMs);
        },
      });

      return !loadController.signal.aborted;
    } catch (error) {
      if (loadController.signal.aborted) return;

      const isError = error instanceof Error;
      if (isError && error.name === "AbortError") return;
      setError(isError ? error.message : String(error));

      if (!isIterative.current) {
        setLogHits([]);
        setDurationMs(undefined);
      }
    } finally {
      setIsLoading(prev => ({ ...prev, [id]: false }));
      clearTimeout(timeoutId);

      if (abortControllerRef.current === loadController) {
        handleUpdateLoadingHits();
      }
    }
  }, [serverUrl, tenant, incrementalTimeoutMs]);

  const resetHits = () => {
    setLogHits([]);
    setDurationMs(undefined);
    setError(undefined);
    isIterative.current = false;
  };

  useEffect(() => {
    return () => {
      abortControllerRef.current.abort();
    };
  }, []);

  useEffect(() => {
    if (hideChart) {
      abortControllerRef.current.abort();
      resetHits();
    }
  }, [hideChart]);

  return {
    logHits,
    isIterative: isIterative.current,
    isLoading: Object.values(isLoading).some(s => s),
    error,
    fetchHits,
    durationMs,
    abort: useCallback(() => abortControllerRef.current?.abort(), []),
    resetHits
  };
};
