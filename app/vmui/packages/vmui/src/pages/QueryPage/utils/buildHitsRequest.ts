import { TenantUrlType } from "../../../hooks/useTenant";
import { FetchHitsParams } from "../hooks/useFetchHits";
import { getHitsTimeParams } from "../../../utils/logs";
import { getDefaultTimezoneOffsetMinutes } from "../../../utils/time";
import { LOGS_LIMIT_HITS, WITHOUT_GROUPING } from "../../../constants/logs";

type HitsRequestHeadersOptions = {
  tenant: TenantUrlType
};

export const buildHitsRequestHeaders = ({ tenant }: HitsRequestHeadersOptions) => {
  return {
    ...tenant
  };
};

export const buildHitsRequestParams = (options: FetchHitsParams) => {
  const { period, query, step, fieldsLimit, field, extraParams } = options;

  const { start, end, step: fallbackStep } = getHitsTimeParams(period);
  const offsetMinutes = getDefaultTimezoneOffsetMinutes();

  const params = new URLSearchParams({
    query: query.trim(),
    step: step || fallbackStep,
    offset: `${offsetMinutes}m`,
    start: start,
    end: end,
    fields_limit: `${fieldsLimit || LOGS_LIMIT_HITS}`,
  });

  if (field && field !== WITHOUT_GROUPING) {
    params.set("field", field);
  }

  return new URLSearchParams([
    ...params,
    ...(extraParams ?? [])
  ]);
};
