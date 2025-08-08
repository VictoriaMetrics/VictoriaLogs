import { FC, useLayoutEffect, useMemo, useRef, useState } from "preact/compat";
import uPlot, { AlignedData } from "uplot";
import dayjs from "dayjs";
import { DATE_TIME_FORMAT } from "../../../../constants/date";
import classNames from "classnames";
import { sortLogHits } from "../../../../utils/logs";
import "./style.scss";

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

const timeFormat = (ts: number) => dayjs(ts * 1000).tz().format(DATE_TIME_FORMAT);

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
      timestamp: `${timeFormat(time)} - ${timeFormat(timeNext)}`,
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
      className={classNames({
        "vm-chart-tooltip": true,
        "vm-chart-tooltip_hits": true,
        "vm-bar-hits-tooltip": true,
      })}
      ref={tooltipRef}
      style={tooltipPosition}
    >
      <div>
        {tooltipData.values.map((item) => (
          <div
            className="vm-chart-tooltip-data"
            key={item.label}
          >
            <span
              className="vm-chart-tooltip-data__marker"
              style={{ background: item.stroke }}
            />
            <p className="vm-bar-hits-tooltip-item">
              <span className="vm-bar-hits-tooltip-item__label">{item.label}</span>
              <span>{item.value.toLocaleString("en-US")}</span>
            </p>
          </div>
        ))}
      </div>

      {tooltipData.values.length > 1 && (
        <div className="vm-chart-tooltip-data">
          <span/>
          <p className="vm-bar-hits-tooltip-item">
            <span className="vm-bar-hits-tooltip-item__label">Total</span>
            <span>{tooltipData.total.toLocaleString("en-US")}</span>
          </p>
        </div>
      )}

      <div className="vm-chart-tooltip-header">
        <div className="vm-chart-tooltip-header__title vm-bar-hits-tooltip__date">
          {tooltipData.timestamp}
        </div>
      </div>
    </div>
  );
};

export default BarHitsTooltip;
