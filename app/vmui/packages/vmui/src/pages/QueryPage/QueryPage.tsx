import { FC, useEffect, useMemo, useState } from "preact/compat";
import QueryPageBody from "./QueryPageBody/QueryPageBody";
import useStateSearchParams from "../../hooks/useStateSearchParams";
import useSearchParamsFromObject from "../../hooks/useSearchParamsFromObject";
import { useFetchLogs } from "./hooks/useFetchLogs";
import Alert from "../../components/Main/Alert/Alert";
import QueryPageHeader from "./QueryPageHeader/QueryPageHeader";
import "./style.scss";
import { ErrorTypes, TimeParams } from "../../types";
import { useTimeDispatch, useTimeState } from "../../state/time/TimeStateContext";
import { getFromStorage, saveToStorage } from "../../utils/storage";
import HitsChart from "./HitsChart/HitsChart";
import { useFetchLogHits } from "./hooks/useFetchLogHits";
import { LOGS_DEFAULT_LIMIT, LOGS_URL_PARAMS } from "../../constants/logs";
import { getTimeperiodForDuration, relativeTimeOptions } from "../../utils/time";
import { useSearchParams } from "react-router-dom";
import { useQueryDispatch, useQueryState } from "../../state/query/QueryStateContext";
import { getUpdatedHistory } from "../../components/QueryHistory/utils";
import { useDebounceCallback } from "../../hooks/useDebounceCallback";
import usePrevious from "../../hooks/usePrevious";
import { filterToExpr } from "../OverviewPage/hooks/useExtraFilters";
import { ExtraFilter } from "../OverviewPage/FiltersBar/types";
import { useHitsChartConfig } from "./HitsChart/hooks/useHitsChartConfig";
import { useLimitGuard } from "./LimitController/useLimitGuard";
import LimitConfirmModal from "./LimitController/LimitConfirmModal";
import { useFetchQueryTime } from "./hooks/useFetchQueryTime";
import { getOverrideValue } from "../../components/Configurators/GlobalSettings/QueryTimeOverride/QueryTimeOverride";
import { GRAPH_QUERY_MODE } from "../../components/Chart/BarHitsChart/types";

const storageLimit = Number(getFromStorage("LOGS_LIMIT"));
const defaultLimit = isNaN(storageLimit) ? LOGS_DEFAULT_LIMIT : storageLimit;

type FetchFlags = { logs: boolean; hits: boolean };

const QueryPage: FC = () => {
  const { queryHistory, queryHasTimeFilter } = useQueryState();
  const queryDispatch = useQueryDispatch();
  const { duration, relativeTime, period: periodState } = useTimeState();
  const timeDispatch = useTimeDispatch();
  const { setSearchParamsFromKeys } = useSearchParamsFromObject();
  const { topHits, groupFieldHits } = useHitsChartConfig();
  const prevTopHits = usePrevious(topHits);
  const prevGroupFieldHits = usePrevious(groupFieldHits);

  const [searchParams] = useSearchParams();

  const hideChart = useMemo(() => Boolean(searchParams.get("hide_chart")), [searchParams]);
  const prevHideChart = usePrevious(hideChart);

  const hideLogs = useMemo(() => Boolean(searchParams.get("hide_logs")), [searchParams]);
  const prevHideLogs = usePrevious(hideLogs);

  const [graphQueryMode] = useStateSearchParams(GRAPH_QUERY_MODE.hits, "graph_mode");
  const prevGraphMode = usePrevious(graphQueryMode);

  const [limit, setLimit] = useStateSearchParams(defaultLimit, LOGS_URL_PARAMS.LIMIT);
  const [query, setQuery] = useStateSearchParams("*", "query");

  const [skipNextPeriodEffect, setSkipNextPeriodEffect] = useState(false);

  const handleChangeLimit = (limit: number) => {
    setLimit(limit);
    setSearchParamsFromKeys({ limit });
    saveToStorage("LOGS_LIMIT", `${limit}`);
  };

  const { beforeFetch, modalProps } = useLimitGuard({ setLimit: handleChangeLimit });

  const updateHistory = () => {
    const history = getUpdatedHistory(query, queryHistory[0]);
    queryDispatch({
      type: "SET_QUERY_HISTORY",
      payload: {
        key: "LOGS_QUERY_HISTORY",
        history: [history],
      }
    });
  };

  const [isUpdatingQuery, setIsUpdatingQuery] = useState(false);
  const [period, setPeriod] = useState<TimeParams>(periodState);
  const [queryError, setQueryError] = useState<ErrorTypes | string>("");

  const { logs, isLoading, error, fetchLogs, abortController, durationMs: queryDuration, queryParams } = useFetchLogs(query, limit);
  const { fetchLogHits, ...dataLogHits } = useFetchLogHits(query);
  const { fetchQueryTime } = useFetchQueryTime(query);

  const fetchData = async (period: TimeParams, flags: FetchFlags) => {
    if (flags.logs) {
      const isSuccess = await fetchLogs({ period, beforeFetch });
      if (!isSuccess) return;
    }

    if (flags.hits) {
      await fetchLogHits({ period, field: groupFieldHits, fieldsLimit: topHits, queryMode: graphQueryMode });
    }
  };

  const debouncedFetchLogs = useDebounceCallback(fetchData, 300);

  const getPeriod = () => {
    const relativeTimeOpts = relativeTimeOptions.find(d => d.id === relativeTime);
    if (!relativeTimeOpts) return periodState;
    const { duration, until } = relativeTimeOpts;
    return getTimeperiodForDuration(duration, until());
  };

  const handleRunQuery = async () => {
    if (!query) {
      setQueryError(ErrorTypes.validQuery);
      return;
    }
    setQueryError("");

    const uiPeriod = getPeriod();
    const apiPeriod = getOverrideValue()
      ? await fetchQueryTime({ query, period: uiPeriod })
      : undefined;

    const newPeriod = apiPeriod ?? uiPeriod;

    queryDispatch({ type: "SET_QUERY_HAS_TIME_FILTER", payload: !!apiPeriod?.hasTimeFilter });
    if (apiPeriod?.hasTimeFilter) {
      setSkipNextPeriodEffect(true);
      timeDispatch({
        type: "SET_PERIOD",
        payload: {
          from: new Date(newPeriod.start * 1000),
          to: new Date(newPeriod.end * 1000)
        }
      });
    }

    setPeriod(newPeriod);
    dataLogHits.abortController.abort?.();
    abortController.abort?.();
    debouncedFetchLogs(newPeriod, { logs: !hideLogs, hits: !hideChart });
    setSearchParamsFromKeys({
      query,
      "g0.range_input": duration,
      "g0.end_input": newPeriod.date,
      "g0.relative_time": relativeTime || "none",
    });
    updateHistory();
  };

  const handleApplyFilter = (val: ExtraFilter) => {
    setQuery(prev => `${filterToExpr(val)} AND ${prev}`);
    setIsUpdatingQuery(true);
  };

  const handleUpdateQuery = () => {
    if (isLoading || dataLogHits.isLoading) {
      abortController.abort?.();
      dataLogHits.abortController.abort?.();
    } else {
      handleRunQuery();
    }
  };

  useEffect(() => {
    if (!query) return;
    if (skipNextPeriodEffect) {
      setSkipNextPeriodEffect(false);
      return;
    }
    handleRunQuery();
  }, [periodState]);

  useEffect(() => {
    if (!isUpdatingQuery) return;
    handleRunQuery();
    setIsUpdatingQuery(false);
  }, [query, isUpdatingQuery]);

  useEffect(() => {
    const topChanged = prevTopHits && (topHits !== prevTopHits);
    const groupChanged = prevGroupFieldHits && (groupFieldHits !== prevGroupFieldHits);
    const becameVisible = prevHideChart && !hideChart;
    const queryModeChanged = prevGraphMode && (graphQueryMode !== prevGraphMode);

    if (!(topChanged || groupChanged || becameVisible || queryModeChanged)) return;

    dataLogHits.abortController.abort?.();
    fetchLogHits({ period, field: groupFieldHits, fieldsLimit: topHits, queryMode: graphQueryMode });
  }, [
    hideChart,
    prevHideChart,
    period,
    groupFieldHits,
    prevGroupFieldHits,
    topHits,
    prevTopHits,
    graphQueryMode,
    prevGraphMode,
    fetchLogHits,
  ]);

  useEffect(() => {
    if (hideLogs || !prevHideLogs) return;
    fetchLogs({ period, beforeFetch });
  }, [hideLogs, prevHideLogs, period, fetchLogs, beforeFetch]);

  return (
    <div className="vm-query-page">
      <LimitConfirmModal
        {...modalProps}
        queryParams={queryParams}
      />

      <QueryPageHeader
        query={query}
        queryDurationMs={hideLogs ? undefined : queryDuration}
        error={queryError}
        limit={limit}
        onChange={setQuery}
        onChangeLimit={handleChangeLimit}
        onRun={handleUpdateQuery}
        isLoading={isLoading || dataLogHits.isLoading}
      />
      {error && <Alert variant="error">{error}</Alert>}
      {queryHasTimeFilter && <Alert variant="warning">
        <p>
          Time range is overridden by the query `_time` filter.
          Remove `_time` from the query to use manual selection.
          Disable query time override in Settings.
        </p>
      </Alert>}
      {!error && (
        <HitsChart
          {...dataLogHits}
          query={query}
          period={period}
          onApplyFilter={handleApplyFilter}
        />
      )}
      <QueryPageBody
        data={logs}
        queryParams={queryParams}
        isLoading={isLoading}
      />
    </div>
  );
};

export default QueryPage;
