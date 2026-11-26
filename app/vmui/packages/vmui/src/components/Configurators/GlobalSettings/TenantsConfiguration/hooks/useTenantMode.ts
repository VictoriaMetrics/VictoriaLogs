import { useSearchParams } from "react-router-dom";
import { getFromStorage, saveToStorage } from "../../../../../utils/storage";
import { useState, useCallback, useRef } from "preact/compat";
import useEventListener from "../../../../../hooks/useEventListener";
import { toPrefixedKey } from "../../../../../utils/storage/utils";
import { useTenants } from "../../../../../hooks/useTenants";

export type TenantMode = "single" | "multitenant";

const DEFAULT_TENANT_MODE: TenantMode = "single";
const URL_KEY = "tenant_mode";
const STORAGE_KEY = "TENANT_MODE";

const isValidTenantMode = (mode: unknown): mode is TenantMode => {
  return mode === "single" || mode === "multitenant";
};

const getStoredTenantMode = (): TenantMode => {
  const mode = getFromStorage(STORAGE_KEY);
  return isValidTenantMode(mode) ? mode : DEFAULT_TENANT_MODE;
};

export const useTenantMode = (availableTenants?: string[]) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [storedMode, setStoredMode] = useState(getStoredTenantMode);

  const { tenants } = useTenants();
  const selectedTenantsRef = useRef(tenants);
  selectedTenantsRef.current = tenants;

  const availableTenantsRef = useRef(availableTenants);
  availableTenantsRef.current = availableTenants;

  const fromUrl = searchParams.get(URL_KEY);
  const tenantMode = isValidTenantMode(fromUrl) ? fromUrl : storedMode;

  const setTenantMode = useCallback((mode: TenantMode): void => {
    if (mode === tenantMode) return;

    saveToStorage(STORAGE_KEY, mode);

    const nextSearchParams = new URLSearchParams(searchParams);
    nextSearchParams.set(URL_KEY, mode);

    if (mode === "single") {
      const value = selectedTenantsRef.current[0] || availableTenantsRef.current?.[0];
      value ? nextSearchParams.set("tenant", value) : nextSearchParams.delete("tenant");
    }

    setSearchParams(nextSearchParams, { replace: true });
  }, [searchParams, setSearchParams, tenantMode]);

  const handleUpdateStorage = (event: StorageEvent) => {
    if (event.key !== toPrefixedKey(STORAGE_KEY)) return;
    setStoredMode(getStoredTenantMode());
  };

  useEventListener("storage", handleUpdateStorage);

  return {
    tenantMode,
    setTenantMode
  };
};
