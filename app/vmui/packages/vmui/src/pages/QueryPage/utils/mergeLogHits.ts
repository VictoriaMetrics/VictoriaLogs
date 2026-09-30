import { LogHits } from "../../../api/types";

export const mergeLogHits = (...arrays: LogHits[][]): LogHits[] => {
  const groups = new Map<string, LogHits>();

  for (const item of arrays.flat()) {
    const key = JSON.stringify([
      Object.entries(item.fields).sort(([a], [b]) => a.localeCompare(b)),
      item._isOther,
    ]);

    const group = groups.get(key);

    if (group) {
      group.timestamps.push(...item.timestamps);
      group.values.push(...item.values);
      group.total += item.total;
    } else {
      groups.set(key, {
        ...item,
        timestamps: [...item.timestamps],
        values: [...item.values],
      });
    }
  }

  return [...groups.values()].sort((a, b) => Number(b._isOther) - Number(a._isOther));
};
