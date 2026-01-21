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
    if (v === null || v === undefined) {
      // null means "no data in this time bucket".
      // Keep null to preserve gaps in the chart and avoid fake zero bars.
      // Do not reset the sum — cumulative growth continues.
      result.push(null);
    } else {
      sum += v;
      result.push(sum);
    }
  }

  return result;
};

