export const arrayEquals = (a: (string | number)[], b: (string | number)[]) => {
  return a.length === b.length && a.every((val, index) => val === b[index]);
};

export function groupByMultipleKeys<T>(items: T[], keys: (keyof T)[]): { keys: string[], values: T[] }[] {
  const groups = items.reduce((result, item) => {
    const groupKeys = keys.map(key => `${String(key)}: ${item[key] || "-"}`);
    const compositeKey = JSON.stringify(groupKeys);

    if (!result.has(compositeKey)) result.set(compositeKey, { keys: groupKeys, values: [] });
    result.get(compositeKey)?.values.push(item);

    return result;
  }, new Map<string, { keys: string[], values: T[] }>());

  return [...groups.values()];
}

export const isDecreasing = (arr: number[]): boolean => {
  if (arr.length < 2) return false;

  return arr.every((v, i) => i === 0 || v < arr[i - 1]);
};

export const arrayMove = <T, >(a: T[], from: number, to: number) => {
  const next = a.slice();
  const [item] = next.splice(from, 1);
  next.splice(to, 0, item);
  return next;
};
