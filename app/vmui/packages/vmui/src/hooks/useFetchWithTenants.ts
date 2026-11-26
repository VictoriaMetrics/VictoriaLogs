import { useTenants } from "./useTenants";
import { useCallback } from "preact/compat";
import { useTenantMode } from "../components/Configurators/GlobalSettings/TenantsConfiguration/hooks/useTenantMode";
import { DEFAULT_TENANT_ID } from "../constants/logs";

type TenantRequestInit = Omit<RequestInit, "body" | "method"> & {
  method: "POST";
  body?: URLSearchParams;
};

export type FetchWithTenants = (
  input: string | URL,
  init: TenantRequestInit,
) => Promise<Response>;

export const useFetchWithTenants = () => {
  const { tenants, tenantsKey } = useTenants();
  const { tenantMode } = useTenantMode();
  const isMultitenant = tenantMode === "multitenant";

  const tenantRequestKey = isMultitenant
    ? `Tenants: ${tenantsKey || "select all"}`
    : `Tenant: ${tenants[0] || DEFAULT_TENANT_ID}`;

  const fetchWithTenants = useCallback<FetchWithTenants>(async (input, init) => {
    const url = new URL(input);
    const headers = new Headers(init.headers);
    const body = new URLSearchParams(init.body);

    if (!isMultitenant) {
      const [accountID, projectID] = (tenants[0] || DEFAULT_TENANT_ID).split(":");
      headers.set("AccountID", accountID);
      headers.set("ProjectID", projectID);
    } else {
      url.pathname = url.pathname.replace(
        "/select/logsql/",
        "/select/multitenant/logsql/"
      );

      const tenantExtraFilters = tenants.map(tenant => {
        const [accountID, projectID] = tenant.split(":");
        return `vl_account_id=${accountID}, vl_project_id=${projectID}`;
      });

      tenants.length && body.append(
        "extra_stream_filters",
        `{${tenantExtraFilters.join(" or ")}}`
      );
    }

    return fetch(url, { ...init, headers, body });
  }, [tenants, isMultitenant]);

  return {
    fetchWithTenants,
    tenantRequestKey
  };
};
