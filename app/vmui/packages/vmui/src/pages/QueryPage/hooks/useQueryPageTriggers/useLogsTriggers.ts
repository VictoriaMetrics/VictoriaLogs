import { useMemo } from "preact/compat";
import { useSearchParams } from "react-router-dom";
import { QueryControllerOptions } from "../useQueryPageController/types";

export const useLogsTriggers = ({ beforeFetch }: QueryControllerOptions) => {
  const [searchParams] = useSearchParams();

  const isLogsHidden = searchParams.get("hide_logs") === "true";

  return useMemo(() => ({
    beforeFetch,
    isLogsHidden,
  }), [beforeFetch, isLogsHidden]);
};
