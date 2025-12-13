import { useEffect, useCallback, useMemo, useRef, useState } from "preact/compat";
import { getLogHitsUrl, getStatsQueryRangeUrl } from "../../../api/logs";
import { ErrorTypes, TimeParams } from "../../../types";
import { LogHits } from "../../../api/types";
import { getHitsTimeParams } from "../../../utils/logs";
import { LOGS_GROUP_BY, LOGS_LIMIT_HITS } from "../../../constants/logs";
import { isEmptyObject } from "../../../utils/object";
import { useTenant } from "../../../hooks/useTenant";
import { useSearchParams } from "react-router-dom";
import { useAppState } from "../../../state/common/StateContext";
import { GRAPH_QUERY_MODE } from "../../../components/Chart/BarHitsChart/types";
import useProcessStatsQueryRange from "./useProcessStatsQueryRange";



type ResponseHits = {
  hits: LogHits[];
}

interface FetchHitsParams {
  query?: string;
  period: TimeParams;
  extraParams?: URLSearchParams;
  field?: string;
  fieldsLimit?: number;
  queryMode?: GRAPH_QUERY_MODE
}

interface OptionsParams extends FetchHitsParams {
  signal: AbortSignal;
}

export const useFetchLogHits = (defaultQuery = "*") => {
  const { serverUrl } = useAppState();
  const tenant = useTenant();
  const [searchParams] = useSearchParams();

  const [logHits, setLogHits] = useState<LogHits[]>([]);
  const [isLoading, setIsLoading] = useState<{ [key: number]: boolean; }>([]);
  const [error, setError] = useState<ErrorTypes | string>();
  const [durationMs, setDurationMs] = useState<number | undefined>();
  const abortControllerRef = useRef(new AbortController());

  const processStatsQueryRange = useProcessStatsQueryRange({ setLogHits, setError });

  const hideChart = useMemo(() => searchParams.get("hide_chart"), [searchParams]);

  const getUrl = useCallback((queryMode: GRAPH_QUERY_MODE) => {
    switch (queryMode) {
      case GRAPH_QUERY_MODE.hits:
        return getLogHitsUrl(serverUrl);
      case GRAPH_QUERY_MODE.stats:
        return getStatsQueryRangeUrl(serverUrl);
    }
  }, [serverUrl]);

  const getOptions = ({ query = defaultQuery, period, extraParams, signal, fieldsLimit, field }: OptionsParams) => {
    const { start, end, step, offset } = getHitsTimeParams(period);

    const params = new URLSearchParams({
      query: query.trim(),
      step: `${step}ms`,
      offset: `${offset}ms`,
      start: start.toISOString(),
      end: end.toISOString(),
      fields_limit: `${fieldsLimit || LOGS_LIMIT_HITS}`,
      field: field || LOGS_GROUP_BY,
    });

    const body = new URLSearchParams([
      ...params,
      ...(extraParams ?? [])
    ]);

    return {
      body,
      signal,
      method: "POST",
      headers: {
        ...tenant,
      },
    };
  };

  const processHits = (data: ResponseHits) => {
    const hitsRaw = data?.hits as LogHits[];

    if (!hitsRaw) {
      const error = "Error: No 'hits' field in response";
      setError(error);
      return [];
    }

    const hits = hitsRaw.map(markIsOther).sort(sortHits);
    setLogHits(hits);

    return hits;
  };

  const fetchLogHits = useCallback(async (params: FetchHitsParams) => {
    const queryMode = params.queryMode || GRAPH_QUERY_MODE.hits;

    abortControllerRef.current.abort();
    abortControllerRef.current = new AbortController();
    const { signal } = abortControllerRef.current;

    const id = Date.now();
    setIsLoading(prev => ({ ...prev, [id]: true }));
    setError(undefined);

    try {
      const options = getOptions({ ...params, signal });
      const url = getUrl(queryMode);
      const response = await fetch(url, options);

      const duration = response.headers.get("vl-request-duration-seconds");
      setDurationMs(duration ? Number(duration) * 1000 : undefined);

      if (!response.ok || !response.body) {
        const text = await response.text();
        try {
          setError(JSON.stringify(JSON.parse(text), null, 2));
        } catch (_e) {
          setError(text);
        }
        setLogHits([]);
        setIsLoading(prev => ({ ...prev, [id]: false }));
        return;
      }

      const data = await response.json();

      switch (queryMode) {
        case GRAPH_QUERY_MODE.hits:
          return processHits(data);
        case GRAPH_QUERY_MODE.stats: {
          const fieldsLimit = +(options.body.get("fields_limit") || LOGS_LIMIT_HITS);
          return processStatsQueryRange(data, fieldsLimit);
        }
      }

    } catch (e) {
      if (e instanceof Error && e.name !== "AbortError") {
        setError(String(e));
        console.error(e);
        setLogHits([]);
      }
    } finally {
      setIsLoading(prev => ({ ...prev, [id]: false }));
    }
  }, [getUrl, defaultQuery, tenant]);

  useEffect(() => {
    return () => {
      abortControllerRef.current.abort();
    };
  }, []);

  useEffect(() => {
    if (hideChart) {
      setLogHits([]);
      setError(undefined);
    }
  }, [hideChart]);

  return {
    logHits,
    isLoading: Object.values(isLoading).some(s => s),
    error,
    fetchLogHits,
    durationMs,
    abortController: abortControllerRef.current
  };
};

// Helper function to check if a hit is "other"
const markIsOther = (hit: LogHits) => ({
  ...hit,
  _isOther: isEmptyObject(hit.fields)
});

// Comparison function for sorting hits
const sortHits = (a: LogHits, b: LogHits) => {
  if (a._isOther !== b._isOther) {
    return a._isOther ? -1 : 1; // "Other" hits first to avoid graph overlap
  }
  return b.total - a.total; // Sort remaining by total for better visibility
};
