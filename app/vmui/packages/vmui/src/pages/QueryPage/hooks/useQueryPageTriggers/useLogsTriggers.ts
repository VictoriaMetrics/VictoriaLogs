import { useMemo } from "preact/compat";
import { useSearchParams } from "react-router-dom";
import { UseQueryPageControllerProps } from "../useQueryPageController";

export const useLogsTriggers = ({ beforeFetch }: UseQueryPageControllerProps) => {
  const [searchParams] = useSearchParams();

  const isLogsHidden = searchParams.get("hide_logs") === "true";

  return useMemo(() => ({
    beforeFetch,
    isLogsHidden,
  }), [beforeFetch, isLogsHidden]);
};
