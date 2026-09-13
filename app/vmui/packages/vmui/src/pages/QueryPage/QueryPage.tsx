import { FC, useEffect, useRef, useState } from "preact/compat";
import QueryPageBody from "./QueryPageBody/QueryPageBody";
import QueryPageHeader from "./QueryPageHeader/QueryPageHeader";
import "./style.scss";
import { ErrorTypes } from "../../types";
import HitsPanel from "./HitsPanel/HitsPanel";
import { useQueryDispatch, useQueryState } from "../../state/query/QueryStateContext";
import { useExtraFilters } from "../../components/ExtraFilters/hooks/useExtraFilters";
import { useHitsChartConfig } from "./HitsPanel/hooks/useHitsChartConfig";
import { useLimitController } from "../../components/Configurators/LogsLimitController/hooks/useLimitController";
import { useLimitGuard } from "../../components/Configurators/LogsLimitController/hooks/useLimitGuard";
import LimitConfirmModal from "../../components/Configurators/LogsLimitController/LimitConfirmModal";
import FilterSidebar from "../../components/FilterSidebar/FilterSidebar";
import { useFilterSidebarVisible } from "../../components/FilterSidebar/hooks/useFilterSidebarVisible";
import classNames from "classnames";
import ExtraFiltersPanel from "../../components/ExtraFilters/ExtraFiltersPanel/ExtraFiltersPanel";
import useDeviceDetect from "../../hooks/useDeviceDetect";
import QueryPageAlerts from "./QueryPageAlerts";
import { useTimePeriod } from "./hooks/useTimePeriod";
import { DEFAULT_QUERY, useQueryController } from "./hooks/useQueryController";
import { useQueryPageController } from "./hooks/useQueryPageController";

const QueryPage: FC = () => {
  const { isMobile } = useDeviceDetect();

  const { queryHasTimeFilter, executeQueryTrigger } = useQueryState();
  const queryDispatch = useQueryDispatch();

  const { period, refreshPeriod } = useTimePeriod();

  const { step: { value: step } } = useHitsChartConfig();

  const { limit, setLimit } = useLimitController();
  const { inputQuery, setInputQuery, inputQueryRef, appliedQuery, applyQuery } = useQueryController();
  const { beforeFetch, modalProps } = useLimitGuard({ setLimit });
  const  { cancelAll, logsRequestState, hitsRequestState } = useQueryPageController({ query: appliedQuery, beforeFetch });

  const isLoading = logsRequestState.isLoading || hitsRequestState.isLoading;

  const [queryError, setQueryError] = useState<ErrorTypes | string>("");

  const { extraFilters, extraParams, addNewFilter, removeFilterByValue, removeFilterByField } = useExtraFilters();
  const { isVisible: isVisibleFilterSidebar, setHidden: onCloseFilterSidebar } = useFilterSidebarVisible();

  const handleUpdateQuery = (nextQuery?: string) => {
    const queryToApply = (nextQuery ?? inputQueryRef.current).trim() || DEFAULT_QUERY;

    setQueryError("");

    applyQuery(queryToApply);
    queryDispatch({ type: "RUN_QUERY" });
  };

  const handleExecuteQuery = (nextQuery?: string) => {
    if (isLoading) {
      cancelAll();
    } else {
      handleUpdateQuery(nextQuery);
    }
  };

  const refreshPeriodRef = useRef(refreshPeriod);
  refreshPeriodRef.current = refreshPeriod;

  useEffect(() => {
    refreshPeriodRef.current();
  }, [executeQueryTrigger]);

  return (
    <div
      className={classNames({
      "vm-query-page": true,
      "vm-query-page_with-sidebar": isVisibleFilterSidebar,
    })}
    >
      <LimitConfirmModal
        {...modalProps}
        queryParams={logsRequestState.queryParams}
      />

      {isVisibleFilterSidebar && (
        <FilterSidebar
          query={appliedQuery}
          extraFilters={extraFilters}
          extraParams={extraParams}
          onAddFilter={addNewFilter}
          onRemoveByValue={removeFilterByValue}
          onRemoveByField={removeFilterByField}
          onClose={onCloseFilterSidebar}
        />
      )}

      <div className="vm-query-page-content">
        <div
          className={classNames({
            "vm-query-page-header": true,
            "vm-block": true,
            "vm-block_mobile": isMobile,
          })}
        >
          <QueryPageHeader
            query={inputQuery}
            queryDurationMs={logsRequestState.durationMs}
            error={queryError}
            limit={limit}
            onChange={setInputQuery}
            onChangeLimit={setLimit}
            onRun={handleExecuteQuery}
            isLoading={isLoading}
          />
          <ExtraFiltersPanel
            extraFilters={extraFilters}
            onRemove={removeFilterByValue}
          />
        </div>

        <QueryPageAlerts
          logsError={logsRequestState.error}
          hitsError={hitsRequestState.error}
          timeOverridden={queryHasTimeFilter}
        />

        {!logsRequestState.error && (
          <HitsPanel
            {...hitsRequestState}
            query={appliedQuery}
            period={period}
            step={step}
          />
        )}
        <QueryPageBody
          data={logsRequestState.logs}
          queryParams={logsRequestState.queryParams}
          isLoading={logsRequestState.isLoading}
        />
      </div>
    </div>
  );
};

export default QueryPage;
