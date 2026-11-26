import { useMemo } from "preact/compat";
import { useAppState } from "../../../../state/common/StateContext";
import { useExtraFilters } from "../../../../components/ExtraFilters/hooks/useExtraFilters";
import { useQueryState } from "../../../../state/query/QueryStateContext";
import { useTimePeriod } from "../useTimePeriod";
import { QueryControllerOptions } from "../useQueryPageController/types";
import { useFetchWithTenants } from "../../../../hooks/useFetchWithTenants";

export const useBaseTriggers = (props: QueryControllerOptions) => {
  const { query } = props;

  const { serverUrl } = useAppState();
  const { tenantRequestKey } = useFetchWithTenants();
  const { period, getCurrentPeriod, setPeriod } = useTimePeriod();
  const { executeQueryTrigger } = useQueryState();

  const { extraParams } = useExtraFilters();
  const extraParamsKey = extraParams.toString();

  return useMemo(() => ({
    serverUrl,
    executeQueryTrigger,
    query,
    period: { start: period.start, end: period.end },
    getCurrentPeriod,
    setPeriod,
    extraParams: new URLSearchParams(extraParamsKey),
  }), [
    query,
    period.start,
    period.end,
    extraParamsKey,
    tenantRequestKey,
    serverUrl,
    executeQueryTrigger,
    getCurrentPeriod,
    setPeriod,
  ]);
};
