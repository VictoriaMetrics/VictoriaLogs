import { ExtraFilter } from "../types";

export const isStreamFilter = (filter: ExtraFilter) => {
  return filter.field === "_stream" || filter.isStream;
};
