import { FC, useLayoutEffect, useMemo, useRef, useState } from "preact/compat";
import uPlot, { AlignedData } from "uplot";
import { sortLogHits } from "../../../../utils/logs";
import { formatNumber } from "../../../../utils/number";
import "./style.scss";
import { getTooltipTimeRangeLines } from "../utils/getTooltipTimeRangeLines";

interface Props {
  data: AlignedData;
  uPlotInst?: uPlot;
  focusDataIdx: number;
}

type TooltipItem = {
  label: string;
  stroke?: string;
  value: number;
  show: boolean;
};

type TooltipData = {
  point: { top: number; left: number };
  values: TooltipItem[];
  total: number;
  timestamp: string;
} | undefined;

const BarHitsTooltip: FC<Props> = ({ data, focusDataIdx, uPlotInst }) => {
  const [isTooltipReady, setTooltipReady] = useState(false);

  const tooltipRef = useRef<HTMLDivElement>(null);

  const tooltipData: TooltipData = useMemo(() => {
    if (!uPlotInst || focusDataIdx < 0 || !data.length || !data[0]?.length) {
      return;
    }

    const time = data[0][focusDataIdx] || 0;
    const step = data[0][1] - data[0][0];
    const timeNext = time + step;
    const values = data.slice(1).map(row => row[focusDataIdx] || 0);
    const series = uPlotInst.series.slice(1);

    let total = 0;

    const tooltipItems = series.reduce((acc, s, i) => {
      if (!s?.show) return acc; // Skip hidden series

      const value = values[i];
      if (value <= 0) return acc; // Skip zero or negative values

      acc.push({
        value,
        label: s.label as string,
        stroke: (s.stroke as (() => string))?.(),
        show: true,
      });

      total += value;

      return acc;
    }, [] as TooltipItem[]);

    tooltipItems.sort(sortLogHits("value"));

    if (!tooltipItems.length) return;

    const point = {
      top: uPlotInst.valToPos?.(tooltipItems[0]?.value ?? 0, "y") || 0,
      left: uPlotInst.valToPos?.(time, "x") || 0,
    };

    return {
      point,
      total,
      values: tooltipItems,
      timestamp: getTooltipTimeRangeLines(time, timeNext, step),
    };
  }, [focusDataIdx, uPlotInst, data]);

  const tooltipPosition = useMemo(() => {
    if (!uPlotInst || !tooltipData || !tooltipRef.current || !isTooltipReady) return;

    const { top, left } = tooltipData.point;
    const uPlotPosition = {
      left: parseFloat(uPlotInst.over.style.left),
      top: parseFloat(uPlotInst.over.style.top)
    };

    const {
      width: uPlotWidth,
      height: uPlotHeight
    } = uPlotInst.over.getBoundingClientRect();

    const {
      width: tooltipWidth,
      height: tooltipHeight
    } = tooltipRef.current.getBoundingClientRect();

    const margin = 50;
    const overflowX = left + tooltipWidth >= uPlotWidth ? tooltipWidth + (2 * margin) : 0;
    const overflowY = top + tooltipHeight >= uPlotHeight ? tooltipHeight + (2 * margin) : 0;

    const position = {
      top: top + uPlotPosition.top + margin - overflowY,
      left: left + uPlotPosition.left + margin - overflowX
    };

    if (position.left < 0) position.left = 20;
    if (position.top < 0) position.top = 20;

    return position;
  }, [tooltipData, uPlotInst, isTooltipReady]);

  useLayoutEffect(() => {
    if (tooltipRef.current) {
      setTooltipReady(true);
    } else {
      setTooltipReady(false);
    }
  }, [tooltipData]);

  if (!tooltipData) return null;

  return (
    <div
      className="vm-chart-tooltip"
      ref={tooltipRef}
      style={tooltipPosition}
    >
      <div className="vm-chart-tooltip-timestamp">
        {tooltipData.timestamp}
      </div>

      <div className="vm-chart-tooltip-data">
        {tooltipData.values.map((item) => (
          <div
            className="vm-chart-tooltip-data-item"
            key={item.label}
          >
            <span
              className="vm-chart-tooltip-data-item__marker"
              style={{ background: item.stroke }}
            />
            <span className="vm-chart-tooltip-data-item__label">{item.label}</span>
            <span className="vm-chart-tooltip-data-item__value">{item.value && formatNumber(item.value)}</span>
          </div>
        ))}

        {tooltipData.values.length > 1 && (
          <div className="vm-chart-tooltip-data-item vm-chart-tooltip-data-item_total">
            <span className="vm-chart-tooltip-data-item__label">Total</span>
            <span className="vm-chart-tooltip-data-item__value">{formatNumber(tooltipData.total)}</span>
          </div>
        )}
      </div>

      <div className="vm-chart-tooltip-tips">
        Click a bar to set the time range
      </div>
    </div>
  );
};

export default BarHitsTooltip;
