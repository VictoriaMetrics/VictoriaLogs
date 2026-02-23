const TARGET_BARS = 96;

// Seven log2 offsets around the base interval.
const SCALE_POWERS = [-3, -2, -1, 0, 1, 2, 3] as const;

// For ranges >= 1 minute, do not use intervals below 500 ms.
const SUBSECOND_CUTOFF_RANGE_MS = 60_000;
const MIN_INTERVAL_FOR_LONG_RANGES_MS = 500;

// Precompute once at module load.
const NICE_INTERVALS_MS = buildNiceIntervalsMs();

/**
 * Input: start/end in UNIX seconds.
 * Output: 7 ascending intervals in milliseconds.
 * Returns [] for invalid or zero-length ranges.
 */
export function generateIntervalsMs(start: number, end: number): number[] {
  const rangeMs = Math.abs(end - start) * 1000;
  if (!Number.isFinite(rangeMs) || rangeMs <= 0) return [];

  const minNice =
    rangeMs >= SUBSECOND_CUTOFF_RANGE_MS ? MIN_INTERVAL_FOR_LONG_RANGES_MS : 1;

  const nice =
    minNice <= 1 ? NICE_INTERVALS_MS : NICE_INTERVALS_MS.filter((v) => v >= minNice);

  const base = snapToNice(rangeMs / TARGET_BARS, nice);
  const picked = SCALE_POWERS.map((p) => snapToNice(base * 2 ** p, nice));

  const uniqSorted = Array.from(new Set(picked)).sort((a, b) => a - b);

  return fillToCount(uniqSorted, nice, 7);
}

function buildNiceIntervalsMs(): number[] {
  const SECOND_MS = 1_000;
  const MINUTE_MS = 60 * SECOND_MS;
  const HOUR_MS = 60 * MINUTE_MS;
  const DAY_MS = 24 * HOUR_MS;
  const APPROX_MONTH_MS = 30 * DAY_MS;
  const APPROX_YEAR_MS = 365 * DAY_MS;

  const multipliers = [1, 2, 5, 10, 15, 30] as const;
  const baseUnits = [
    1, // ms
    SECOND_MS,
    MINUTE_MS,
    HOUR_MS,
    DAY_MS,
    7 * DAY_MS, // week
    APPROX_MONTH_MS, // ~month
    APPROX_YEAR_MS, // ~year
  ] as const;

  const intervals = new Set<number>();

  // Base intervals: 1/2/5/10/15/30 * unit.
  for (const unit of baseUnits) {
    for (const mult of multipliers) {
      intervals.add(unit * mult);
    }
  }

  // Extra useful intervals.
  const extras = [
    100, // 100ms
    200, // 200ms
    250, // 250ms
    500, // 500ms
    45 * SECOND_MS, // 45s
    12 * MINUTE_MS, // 12m
    4 * HOUR_MS, // 4h
    6 * HOUR_MS, // 6h
    12 * HOUR_MS, // 12h
    2 * DAY_MS, // 2d
    3 * DAY_MS, // 3d
    14 * DAY_MS, // 14d
    90 * DAY_MS, // ~3mo
    180 * DAY_MS, // ~6mo
    2 * APPROX_YEAR_MS, // 2y
    5 * APPROX_YEAR_MS, // 5y
    10 * APPROX_YEAR_MS, // 10y
  ] as const;

  for (const value of extras) {
    intervals.add(value);
  }

  return [...intervals].sort((a, b) => a - b);
}

function snapToNice(x: number, nice: readonly number[]): number {
  const first = nice[0];
  const last = nice[nice.length - 1];

  if (x <= first) return first;
  if (x >= last) return last;

  const i = lowerBound(nice, x);
  const upper = nice[i];
  const lower = nice[i - 1];

  // Prefer the lower value on ties.
  return x - lower <= upper - x ? lower : upper;
}

function fillToCount(
  baseSortedAsc: readonly number[],
  nice: readonly number[],
  count: number,
): number[] {
  if (count <= 0) return [];
  if (nice.length === 0) return [];
  if (baseSortedAsc.length === 0) return nice.slice(0, count);

  if (baseSortedAsc.length >= count) {
    return baseSortedAsc.slice(0, count);
  }

  const set = new Set(baseSortedAsc);
  const out = [...baseSortedAsc];

  // Expand from the median anchor to nearby values in `nice`.
  const anchor = baseSortedAsc[Math.floor(baseSortedAsc.length / 2)];
  const idx = lowerBound(nice, anchor);

  const anchorExistsInNice = idx < nice.length && nice[idx] === anchor;

  let left = idx - 1;
  let right = anchorExistsInNice ? idx + 1 : idx;

  while (out.length < count && (left >= 0 || right < nice.length)) {
    if (left >= 0) {
      const v = nice[left--];
      if (!set.has(v)) {
        set.add(v);
        out.push(v);
      }
    }

    if (out.length >= count) break;

    if (right < nice.length) {
      const v = nice[right++];
      if (!set.has(v)) {
        set.add(v);
        out.push(v);
      }
    }
  }

  return out.sort((a, b) => a - b);
}

function lowerBound(values: readonly number[], target: number): number {
  let left = 0;
  let right = values.length;

  while (left < right) {
    const mid = (left + right) >>> 1;

    if (values[mid] < target) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  return left;
}
