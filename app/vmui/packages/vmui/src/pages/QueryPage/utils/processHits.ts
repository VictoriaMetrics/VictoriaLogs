import { LogHits } from "../../../api/types";
import { isEmptyObject } from "../../../utils/object";

type ResponseHits = {
  hits: LogHits[];
}

export const processHits = (data: ResponseHits) => {
  const hitsRaw = data?.hits as LogHits[];

  if (!hitsRaw) {
    throw new Error("Error: No 'hits' field in response");
  }

  return hitsRaw.map(markIsOther).sort(sortHits);
};

// Helper function to check if a hit is "other"
const markIsOther = (hit: LogHits) => ({
  ...hit,
  _isOther: isEmptyObject(hit.fields)
});

// Comparison function for sorting hits
const sortHits = (a: LogHits, b: LogHits) => {
  if (a._isOther !== b._isOther) {
    return a._isOther ? -1 : 1; // "Other" hits first to avoid graph overlap
  }
  return b.total - a.total; // Sort remaining by total for better visibility
};
