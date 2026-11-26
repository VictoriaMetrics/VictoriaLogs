import { TimeParams } from "../types";

export const getTenantsUrl = (server: string, period: TimeParams) => {
  const params = new URLSearchParams({
    start: period.start.toString(),
    end: period.end.toString(),
  });

  return `${server}/select/tenant_ids?${params}`;
};
