import { ExtraFilter } from "../types";
import { STREAM_FIELD } from "../../../constants/logFields";

export const isStreamFilter = (filter: ExtraFilter) => {
  return filter.field === STREAM_FIELD || filter.isStream;
};
