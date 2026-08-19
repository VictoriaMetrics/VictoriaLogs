import { AppConfig } from "../types";

/**
 * Tenant aliases are human-readable names for `accountID:projectID` pairs.
 * They come from the `tenantAliases` field of vmui `config.json`, which is
 * populated by the `-vmui.tenantAliases` command-line flag at VictoriaLogs.
 * Aliases affect only the way tenants are displayed - the tenant itself is
 * still identified by the `accountID` and `projectID` query args.
 */
export type TenantAliases = Record<string, string>;

/** Returns canonical `accountID:projectID` form of the given tenant id, or an empty string if it is invalid. */
export const normalizeTenantId = (value: string): string => {
  const parts = `${value}`.trim().split(":");
  if (parts.length > 2) return "";
  const [accountId, projectId = "0"] = parts;
  const isUint = (s: string) => /^\d+$/.test(s.trim());
  if (!isUint(accountId) || !isUint(projectId)) return "";
  return `${Number(accountId)}:${Number(projectId)}`;
};

/** Extracts tenant aliases from the app config, dropping malformed entries. */
export const parseTenantAliases = (appConfig?: AppConfig): TenantAliases => {
  const raw = appConfig?.tenantAliases;
  if (!raw || typeof raw !== "object") return {};

  return Object.entries(raw).reduce<TenantAliases>((acc, [key, value]) => {
    const tenantId = normalizeTenantId(key);
    const alias = typeof value === "string" ? value.trim() : "";
    if (tenantId && alias) acc[tenantId] = alias;
    return acc;
  }, {});
};

/** Returns the alias for the given tenant id, or the tenant id itself if there is no alias. */
export const getTenantLabel = (tenantId: string, aliases: TenantAliases): string => {
  return aliases[normalizeTenantId(tenantId) || tenantId] || tenantId;
};

/** Returns the string the tenant is searched by - both the alias and the raw tenant id are matched. */
export const getTenantSearchString = (tenantId: string, aliases: TenantAliases): string => {
  const label = getTenantLabel(tenantId, aliases);
  return label === tenantId ? tenantId : `${label} ${tenantId}`;
};
