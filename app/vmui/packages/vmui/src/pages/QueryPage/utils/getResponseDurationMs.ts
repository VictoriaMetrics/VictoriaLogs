import { secondsToMilliseconds } from "../../../utils/time";

export const getResponseDurationMs = (response: Response): number | undefined => {
  const value = response.headers.get("vl-request-duration-seconds");

  if (!value?.trim()) return undefined;

  const seconds = Number(value);
  return Number.isFinite(seconds) && seconds >= 0 ? secondsToMilliseconds(seconds) : undefined;
};
