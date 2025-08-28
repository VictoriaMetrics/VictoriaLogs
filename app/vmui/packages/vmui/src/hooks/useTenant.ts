import { useMemo } from "preact/compat";
import { useSearchParams } from "react-router-dom";

export const useTenant = () => {
  const [searchParams] = useSearchParams();

  const accountID = searchParams.get("accountID") || "0";
  const projectID = searchParams.get("projectID") || "0";

  return useMemo(() => ({
    AccountID: accountID,
    ProjectID: projectID,
  }), [accountID, projectID]);
};
