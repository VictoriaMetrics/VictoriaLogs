import { useMemo } from "preact/compat";
import { useSearchParams } from "react-router-dom";

export const useTenant = () => {
  const [searchParams] = useSearchParams();

  return useMemo(() => ({
    AccountID: searchParams.get("accountID") || "0",
    ProjectID: searchParams.get("projectID") || "0",
  }), [searchParams]);
};
