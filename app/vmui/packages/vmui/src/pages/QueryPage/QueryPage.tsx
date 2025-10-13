import { FC, useEffect, useMemo, useState } from "preact/compat";
import QueryPageBody from "./QueryPageBody/QueryPageBody";
import useStateSearchParams from "../../hooks/useStateSearchParams";
import useSearchParamsFromObject from "../../hooks/useSearchParamsFromObject";
import { useFetchLogs } from "./hooks/useFetchLogs";
import Alert from "../../components/Main/Alert/Alert";
import QueryPageHeader from "./QueryPageHeader/QueryPageHeader";
import "./style.scss";
import { ErrorTypes, TimeParams } from "../../types";
import { useTimeState } from "../../state/time/TimeStateContext";
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

const storageLimit = Number(getFromStorage("LOGS_LIMIT"));
const defaultLimit = isNaN(storageLimit) ? LOGS_DEFAULT_LIMIT : storageLimit;

type FetchFlags = { logs: boolean; hits: boolean };

const QueryPage: FC = () => {
  const { queryHistory } = useQueryState();
  const queryDispatch = useQueryDispatch();
  const { duration, relativeTime, period: periodState } = useTimeState();
  const { setSearchParamsFromKeys } = useSearchParamsFromObject();
  const { topHits, groupFieldHits } = useHitsChartConfig();
  const [searchParams] = useSearchParams();

  const hideChart = useMemo(() => Boolean(searchParams.get("hide_chart")), [searchParams]);

  const hideLogs = useMemo(() => Boolean(searchParams.get("hide_logs")), [searchParams]);
  const prevHideLogs = usePrevious(hideLogs);

  const [limit, setLimit] = useStateSearchParams(defaultLimit, LOGS_URL_PARAMS.LIMIT);
  const [query, setQuery] = useStateSearchParams("*", "query");

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

  const { logs, isLoading, error, fetchLogs, abortController, durationMs: queryDuration } = useFetchLogs(query, limit);
  const { fetchLogHits, ...dataLogHits } = useFetchLogHits(query);

  const fetchData = async (period: TimeParams, flags: FetchFlags) => {
    if (flags.logs) {
      const isSuccess = await fetchLogs({ period, beforeFetch });
      if (!isSuccess) return;
    }

    if (flags.hits) {
      await fetchLogHits({ period, field: groupFieldHits, fieldsLimit: topHits });
    }
  };

  const debouncedFetchLogs = useDebounceCallback(fetchData, 300);

  const getPeriod = () => {
    const relativeTimeOpts = relativeTimeOptions.find(d => d.id === relativeTime);
    if (!relativeTimeOpts) return periodState;
    const { duration, until } = relativeTimeOpts;
    return getTimeperiodForDuration(duration, until());
  };

  const handleRunQuery = () => {
    if (!query) {
      setQueryError(ErrorTypes.validQuery);
      return;
    }
    setQueryError("");

    const newPeriod = getPeriod();
    setPeriod(newPeriod);
    dataLogHits.abortController.abort();
    abortController.abort();
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
    handleRunQuery();
  }, [periodState]);

  useEffect(() => {
    if (!isUpdatingQuery) return;
    handleRunQuery();
    setIsUpdatingQuery(false);
  }, [query, isUpdatingQuery]);

  useEffect(() => {
    if (hideChart) return;
    fetchLogHits({ period, field: groupFieldHits, fieldsLimit: topHits });
  }, [hideChart, period, groupFieldHits, topHits]);

  useEffect(() => {
    if (!hideLogs && prevHideLogs) {
      fetchLogs({ period, beforeFetch });
    }
  }, [hideLogs, prevHideLogs, period]);

  return (
    <div className="vm-query-page">
      <LimitConfirmModal {...modalProps}/>

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
        isLoading={isLoading}
      />
    </div>
  );
};

export default QueryPage;
