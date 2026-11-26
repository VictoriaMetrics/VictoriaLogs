import { getLogHitsUrl } from "../../../api/logs";
import { getResponseError } from "./getResponseError";
import { processHits } from "./processHits";
import { getResponseDurationMs } from "./getResponseDurationMs";
import { FetchWithTenants } from "../../../hooks/useFetchWithTenants";

export type FetchHitsOptions = {
  url: string;
  body: URLSearchParams;
  headers?: HeadersInit;
  signal: AbortSignal;
  method: "POST";
}

export const fetchHitsOnce = async ({ url, ...init }: FetchHitsOptions, fetchWithTenants: FetchWithTenants) => {
  const serverUrl = getLogHitsUrl(url);
  const response = await fetchWithTenants(serverUrl, init);

  if (!response.ok || !response.body) {
    throw new Error(await getResponseError(response));
  }

  const data = await response.json();

  return {
    hits: processHits(data),
    durationMs: getResponseDurationMs(response),
  };
};
