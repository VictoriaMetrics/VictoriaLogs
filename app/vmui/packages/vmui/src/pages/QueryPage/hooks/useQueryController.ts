import { useCallback, useEffect, useRef, useState } from "preact/compat";
import { useSearchParams } from "react-router-dom";
import { LOGS_URL_PARAMS } from "../../../constants/logs";

export const DEFAULT_QUERY = "*";
const PARAM_KEY = LOGS_URL_PARAMS.QUERY;

export const useQueryController = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const appliedQuery = searchParams.get(PARAM_KEY) || DEFAULT_QUERY;
  const [inputQuery, setInputQuery] = useState(appliedQuery);

  const inputQueryRef = useRef(inputQuery);

  const applyQuery = useCallback((query?: string) => {
    const nextQuery = (query ?? inputQueryRef.current).trim() || DEFAULT_QUERY;
    const prevQuery = appliedQuery;

    setInputQuery(nextQuery);

    setSearchParams(prev => {
      const nextParams = new URLSearchParams(prev);
      nextParams.set(PARAM_KEY, nextQuery);
      return nextParams;
    });

    return { prevQuery, nextQuery };
  }, [appliedQuery, setSearchParams]);

  useEffect(() => {
    inputQueryRef.current = inputQuery;
  }, [inputQuery]);

  useEffect(() => {
    setInputQuery(appliedQuery);
  }, [appliedQuery]);

  return {
    inputQuery,
    setInputQuery,
    inputQueryRef,

    appliedQuery,
    applyQuery,

    isDraft: inputQuery !== appliedQuery,
  };
};
