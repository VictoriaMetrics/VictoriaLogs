import { AlignedData, TypedArray } from "uplot";

export const cumulativeMatrix = (data: AlignedData): AlignedData => {
  return data.map((row, i) => {
    if (i === 0) return row; // time axis
    return cumulativeHits(row);
  }) as AlignedData;
};

const cumulativeHits = (arr: TypedArray | number[] | (number | null | undefined)[]) => {
  const result: (number | null)[] = [];
  let sum = 0;

  for (const v of arr) {
    sum += v ?? 0;
    result.push(sum);
  }

  return result;
};

