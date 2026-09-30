import { FetchHitsParams, FetchHitsResult, useFetchHits } from "../useFetchHits";

export const useHitsController = () => {
  const { fetchHits, ...hitsRequestState } = useFetchHits();

  const runHits = async (params: FetchHitsParams): Promise<FetchHitsResult> => {
    return fetchHits(params);
  };

  return {
    runHits,
    ...hitsRequestState,
  };
};
