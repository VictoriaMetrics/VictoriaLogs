import { BeforeFetch } from "../useFetchLogs";
import { useBaseTriggers, useHitsTriggers, useLogsTriggers } from "../useQueryPageTriggers";

export type QueryControllerOptions = {
  query: string;
  beforeFetch?: BeforeFetch;
};

export type LogsPhase = "idle" | "preparing" | "loading" | "retryPending";

export type BaseTriggers = ReturnType<typeof useBaseTriggers>;
export type LogsTriggers = ReturnType<typeof useLogsTriggers>;
export type HitsTriggers = ReturnType<typeof useHitsTriggers>;

export type TriggersState = {
  baseTriggers: BaseTriggers;
  logsTriggers: LogsTriggers;
  hitsTriggers: HitsTriggers;
};
