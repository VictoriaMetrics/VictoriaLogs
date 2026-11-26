import { useMemo } from "preact/compat";
import { useSearchParams } from "react-router-dom";
import { TenantMode } from "../components/Configurators/GlobalSettings/TenantsConfiguration/hooks/useTenantMode";

export type TenantUrlType = {
  AccountID: string
  ProjectID: string
}

// Supports legacy accountID/projectID URLs used before tenant multi-select was introduced in November 2026.
export const getLegacyTenantSearchParams = (source: URLSearchParams): URLSearchParams => {
  const params = new URLSearchParams();
  const accountID = source.get("accountID");
  const projectID = source.get("projectID");

  if (accountID || projectID) {
    params.set("tenant", `${accountID || "0"}:${projectID || "0"}`);
  }

  return params;
};

export const getTenantSearchParams = (source: URLSearchParams): URLSearchParams => {
  const tenants = source.getAll("tenant");
  const tenantMode = source.get("tenant_mode");

  const params = tenants.length
    ? new URLSearchParams()
    : getLegacyTenantSearchParams(source);


  if (tenantMode) params.set("tenant_mode", tenantMode);

  tenants.forEach(tenant => params.append("tenant", tenant));

  return params;
};

export const useTenants = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const tenantValues = searchParams.getAll("tenant");
  const fallbackTenants = getLegacyTenantSearchParams(searchParams).getAll("tenant");

  // Memoize tenants to avoid unnecessary re-renders when the search params change but the tenants remain the same.
  const selectedTenants = tenantValues.length ? tenantValues : fallbackTenants;
  const tenantsStr = JSON.stringify(selectedTenants.sort());
  const tenants = useMemo<string[]>(() => JSON.parse(tenantsStr), [tenantsStr]);
  const tenantsKey = tenants.join(", ");

  const setTenants = (nextTenants: string[], tenantMode: TenantMode) => {
    const next = new URLSearchParams(searchParams);

    // Removes legacy parameters when updating the tenant selection.
    next.delete("accountID");
    next.delete("projectID");

    next.delete("tenant");

    next.set("tenant_mode", tenantMode);

    nextTenants.forEach(tenant => next.append("tenant", tenant));
    setSearchParams(next);

    return nextTenants;
  };

  return {
    tenants,
    tenantsKey,
    setTenants,
  };
};
