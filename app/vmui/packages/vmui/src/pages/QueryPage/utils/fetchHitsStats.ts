import { FetchHitsOptions } from "./fetchHitsOnce";
import { getStatsQueryRangeUrl } from "../../../api/logs";
import { getResponseError } from "./getResponseError";
import { LOGS_LIMIT_HITS } from "../../../constants/logs";
import { processStatsQueryRange } from "./processStatsQueryRange";
import { getResponseDurationMs } from "./getResponseDurationMs";

export const fetchHitsStats = async ({ url, ...init }: FetchHitsOptions) => {
  const serverUrl = getStatsQueryRangeUrl(url);
  const response = await fetch(serverUrl, init);

  if (!response.ok || !response.body) {
    throw new Error(await getResponseError(response));
  }

  const data = await response.json();
  const fieldsLimit = +(init.body.get("fields_limit") || LOGS_LIMIT_HITS);

  return {
    hits: processStatsQueryRange(data, fieldsLimit),
    durationMs: getResponseDurationMs(response),
  };
};
