import { useMemo } from "preact/compat";
import { useAppState } from "../state/common/StateContext";
import { getTenantLabel, parseTenantAliases, TenantAliases } from "../utils/tenant";

type TenantAliasesResult = {
  aliases: TenantAliases;
  getLabel: (tenantId: string) => string;
}

/** Provides tenant aliases from vmui `config.json` together with a display-name resolver. */
export const useTenantAliases = (): TenantAliasesResult => {
  const { appConfig } = useAppState();

  const aliases = useMemo(() => parseTenantAliases(appConfig), [appConfig]);

  return useMemo(() => ({
    aliases,
    getLabel: (tenantId: string) => getTenantLabel(tenantId, aliases),
  }), [aliases]);
};
