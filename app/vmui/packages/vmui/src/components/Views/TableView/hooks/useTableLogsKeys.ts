import { useMemo } from "preact/compat";
import { getAllStreamKeys } from "../../../../utils/logs";
import { DEFAULT_STREAM_FIELDS } from "../../../../constants/logs";
import { Logs } from "../../../../api/types";

export type LogsColumnStats = {
  frequencyPercent: number;
  uniqValuesCount: number;
};

type KeyAgg = {
  count: number;
  uniqSet: Set<unknown>;
};

export const useTableLogsKeys = (data: Logs[]) => {
  return useMemo(() => {
    const total = data.length || 1;

    const aggByKey = new Map<string, KeyAgg>();

    for (const item of data) {
      for (const key of Object.keys(item)) {
        let agg = aggByKey.get(key);
        if (!agg) {
          agg = { count: 0, uniqSet: new Set<unknown>() };
          aggByKey.set(key, agg);
        }

        agg.count += 1;
        agg.uniqSet.add((item)[key]);
      }
    }

    const statsByKey = new Map<string, LogsColumnStats>();
    for (const [key, agg] of aggByKey) {
      statsByKey.set(key, {
        frequencyPercent: (agg.count / total) * 100,
        uniqValuesCount: agg.uniqSet.size,
      });
    }

    const streamKeys = getAllStreamKeys(data);

    return {
      statsByKey,
      columnKeys: Array.from(aggByKey.keys()),
      streamKeys: streamKeys.length ? streamKeys : DEFAULT_STREAM_FIELDS,
    };
  }, [data]);
};
