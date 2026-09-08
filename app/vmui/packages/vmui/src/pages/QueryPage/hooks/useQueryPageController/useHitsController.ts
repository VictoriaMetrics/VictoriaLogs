import { FetchHitsParams, useFetchHits } from "../useFetchHits";

export const useHitsController = () => {
  const { fetchHits, ...hitsRequestState } = useFetchHits();

  const runHits = async (params: FetchHitsParams): Promise<boolean> => {
    hitsRequestState.abort();

    try {
      const isSuccess = await fetchHits(params);

      return Boolean(isSuccess);
    } catch {
      return false;
    }
  };

  return {
    runHits,
    ...hitsRequestState,
  };
};
