import qs from "qs";
import { MAX_QUERY_FIELDS } from "../constants/logs";

export const getQueryStringValue = (
  key: string,
  defaultValue?: unknown,
): unknown => {
  const queryString = window.location.hash.split("?")[1];
  const values = qs.parse(queryString, { ignoreQueryPrefix: true });
  return values?.[key] ?? defaultValue ?? "";
};

export const getQueryArray = (): string[] => {
  const queryString = window.location.hash.split("?")[1] || "";
  const queryLength = queryString.match(/g\d+\.expr/g)?.length || 1;
  return new Array(queryLength > MAX_QUERY_FIELDS ? MAX_QUERY_FIELDS : queryLength)
    .fill(1)
    .map((q, i) => getQueryStringValue(`g${i}.expr`, "") as string);
};

type MergeMode = "append" | "overwrite";

export const mergeSearchParams = (a: URLSearchParams, b: URLSearchParams, mode: MergeMode) => {
  const out = new URLSearchParams(a);
  if (mode === "append") {
    b.forEach((v, k) => out.append(k, v));
  } else { // 'overwrite'
    for (const [k, v] of b) out.set(k, v);
  }
  return out;
};
