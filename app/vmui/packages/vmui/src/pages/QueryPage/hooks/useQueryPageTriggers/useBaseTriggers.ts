import { useMemo } from "preact/compat";
import { useAppState } from "../../../../state/common/StateContext";
import { useTenant } from "../../../../hooks/useTenant";
import { useExtraFilters } from "../../../../components/ExtraFilters/hooks/useExtraFilters";
import { useQueryState } from "../../../../state/query/QueryStateContext";
import { useTimePeriod } from "../useTimePeriod";
import { UseQueryPageControllerProps } from "../useQueryPageController";

export const useBaseTriggers = (props: UseQueryPageControllerProps) => {
  const { query } = props;

  const { serverUrl } = useAppState();
  const tenant = useTenant();
  const { period, getCurrentPeriod, setPeriod } = useTimePeriod();
  const { executeQueryTrigger } = useQueryState();

  const { extraParams } = useExtraFilters();
  const extraParamsKey = extraParams.toString();

  return useMemo(() => ({
    tenant,
    serverUrl,
    executeQueryTrigger,
    query,
    period: { start: period.start, end: period.end },
    getCurrentPeriod,
    setPeriod,
    extraParams: new URLSearchParams(extraParamsKey),
  }), [query, period.start, period.end, extraParamsKey, tenant, serverUrl, executeQueryTrigger, getCurrentPeriod, setPeriod]);
};
