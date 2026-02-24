const DEFAULT_LOCALE = "en-US" as const;

export const formatNumber = (
  value: number,
  options?: Intl.NumberFormatOptions,
) => {
  const ops = { useGrouping: true, ...options };
  return new Intl.NumberFormat(DEFAULT_LOCALE, ops).format(value);
};

export const formatNumberShort = (value: number) => {
  return formatNumber(value, {
    notation: "compact",
    compactDisplay: "short",
    maximumFractionDigits: 1,
  });
};

export const formatPercent = (p: number | null, fractionDigits?: number) => {
  if (p == null) return "-";
  const abs = Math.abs(p);
  if (abs >= 1) return p.toFixed(fractionDigits ?? 1) + "%";
  if (abs >= 0.01) return p.toFixed(fractionDigits ?? 2) + "%";
  if (p === 0) return "0%";
  return "<0.01%";
};
