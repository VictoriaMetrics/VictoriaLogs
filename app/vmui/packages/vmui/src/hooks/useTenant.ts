import { useMemo } from "preact/compat";
import { useSearchParams } from "react-router-dom";

export type TenantUrlType = {
  AccountID: string
  ProjectID: string
}

export const getTenantSearchParams = (source: URLSearchParams): URLSearchParams => {
  const params = new URLSearchParams();
  const accountID = source.get("accountID");
  const projectID = source.get("projectID");
  if (accountID) params.set("accountID", accountID);
  if (projectID) params.set("projectID", projectID);
  return params;
};

export const useTenant = (): TenantUrlType => {
  const [searchParams] = useSearchParams();

  const accountID = searchParams.get("accountID") || "0";
  const projectID = searchParams.get("projectID") || "0";

  return useMemo(() => ({
    AccountID: accountID,
    ProjectID: projectID,
  }), [accountID, projectID]);
};
