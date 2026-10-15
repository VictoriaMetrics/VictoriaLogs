import { FC, useEffect, useMemo, useRef, useState } from "preact/compat";
import uPlot, { AlignedData } from "uplot";
import { GraphOptions } from "../types";
import usePlotScale from "../../../../hooks/uplot/usePlotScale";
import useReadyChart from "../../../../hooks/uplot/useReadyChart";
import useZoomChart from "../../../../hooks/uplot/useZoomChart";
import stack from "../../../../utils/uplot/stack";
import useBarHitsOptions, { getLabelFromLogHit } from "../hooks/useBarHitsOptions";
import { LegendLogHits, LogHits } from "../../../../api/types";
import { addSeries, delSeries, setBand } from "../../../../utils/uplot";
import classNames from "classnames";
import BarHitsTooltip from "../BarHitsTooltip/BarHitsTooltip";
import { TimeParams, TimePeriod } from "../../../../types";
import BarHitsLegend from "../BarHitsLegend/BarHitsLegend";
import { sortLogHits } from "../../../../utils/logs";
import { useAppState } from "../../../../state/common/StateContext";
import { useTimeState } from "../../../../state/time/TimeStateContext";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import { cumulativeMatrix } from "../../../../utils/uplot/cumulative";
import { Size, useResizeObserver } from "../../../../hooks/useResizeObserver";
import BarHitsLoadingOverlay from "../BarHitsLoadingOverlay/BarHitsLoadingOverlay";

interface Props {
  logHits: LogHits[];
  totalHits: number;
  data: AlignedData;
  isIterative: boolean;
  period: TimeParams;
  setPeriod: (nextPeriod: TimePeriod) => void;
  graphOptions: GraphOptions;
}

const BarHitsPlot: FC<Props> = ({
  graphOptions,
  logHits,
  totalHits,
  data: _data,
  isIterative,
  period,
  setPeriod
}: Props) => {
  const { isMobile } = useDeviceDetect();
  const { isDarkTheme } = useAppState();
  const { timezone } = useTimeState();
  const containerRef = useRef<HTMLDivElement>(null);
  const uPlotRef = useRef<HTMLDivElement>(null);
  const [uPlotInst, setUPlotInst] = useState<uPlot>();

  const [containerSize, setContainerSize] = useState<Size>({ width: 0, height: 0 });

  const { xRange, setPlotScale } = usePlotScale({ period, setPeriod });
  const { onReadyChart, isPanning } = useReadyChart(setPlotScale);
  useZoomChart({ uPlotInst, element: uPlotRef, xRange, setPlotScale });

  const transformedData = useMemo(() => {
    if (!graphOptions.cumulative) return _data;

    const cumulativeData = cumulativeMatrix(_data);

    return cumulativeData.map((row, i) =>
      // filter out loading series from cumulative calculation, but keep them in the data for rendering
      i > 0 && logHits[i - 1]?._isLoading ? _data[i] : row
    ) as AlignedData;
  }, [graphOptions.cumulative, _data, logHits]);

  const { data, bands } = useMemo(() => {
    if (graphOptions.stacked) {
      // filter out loading series from stack calculation, but keep them in the data for rendering
      return stack(transformedData, i => !!logHits[i - 1]?._isLoading);
    }

    return { data: transformedData, bands: [] };
  }, [graphOptions.stacked, transformedData, logHits]);

  const { options, series, focusDataIdx, getLoadingBarRect } = useBarHitsOptions({
    data,
    logHits,
    bands,
    xRange,
    containerSize,
    onReadyChart,
    setPlotScale,
    graphOptions,
    timezone,
    setPeriod
  });

  const legendDetails: LegendLogHits[] = useMemo(() => {
    return logHits
      .filter(hit => !hit._isLoading)
      .map((hit) => {
        const label = getLabelFromLogHit(hit);

        const legendItem: LegendLogHits = {
          label,
          isOther: hit._isOther,
          fields: hit.fields,
          total: hit.total || 0,
          totalHits,
          stroke: series.find((s) => s.label === label)?.stroke,
        };

        return legendItem;
      }).sort(sortLogHits("total"));
  }, [logHits, totalHits, series]);

  const isSingleOtherSeries = useMemo(() => {
    return legendDetails.length < 2 && legendDetails.every(l => l.isOther);
  }, [legendDetails]);

  useEffect(() => {
    if (!uPlotInst) return;

    const oldSeriesMap = new Map(uPlotInst.series.map(s => [s.label, s]));

    const syncedSeries = series.map(s => {
      const old = oldSeriesMap.get(s.label);
      return old ? { ...s, show: old.show } : s;
    });

    delSeries(uPlotInst);
    addSeries(uPlotInst, syncedSeries, true);
    setBand(uPlotInst, syncedSeries);
    uPlotInst.redraw();
  }, [series, uPlotInst]);

  useEffect(() => {
    if (!uPlotInst) return;
    uPlotInst.delBand();
    bands.forEach(band => {
      uPlotInst.addBand(band);
    });
    uPlotInst.redraw();
  }, [bands]);

  useEffect(() => {
    if (!uPlotRef.current) return;
    const uplot = new uPlot(options, data, uPlotRef.current);
    setUPlotInst(uplot);
    return () => uplot.destroy();
  }, [uPlotRef.current, isDarkTheme, timezone]);

  useEffect(() => {
    if (!uPlotInst) return;
    uPlotInst.scales.x.range = () => [xRange.min, xRange.max];
    uPlotInst.redraw();
  }, [xRange]);

  useEffect(() => {
    if (!uPlotInst) return;
    uPlotInst.setSize({
      width: containerSize.width || window.innerWidth / 2,
      height: containerSize.height || window.innerHeight / 4,
    });
    uPlotInst.redraw();
  }, [containerSize]);

  useEffect(() => {
    if (!uPlotInst) return;
    uPlotInst.setData(data);
    uPlotInst.redraw();
  }, [data]);

  useResizeObserver({ ref: containerRef, onResize: setContainerSize });

  return (
    <>
      <div
        className={classNames({
          "vm-bar-hits-chart": true,
          "vm-bar-hits-chart_panning": isPanning
        })}
        ref={containerRef}
      >
        <div
          className="vm-bar-hits-chart__u-plot"
          ref={uPlotRef}
        />

        {uPlotInst && (
          <BarHitsLoadingOverlay
            uPlotInst={uPlotInst}
            getLoadingBarRect={getLoadingBarRect}
          />
        )}

        {!isMobile && (
          <BarHitsTooltip
            uPlotInst={uPlotInst}
            data={transformedData}
            focusDataIdx={focusDataIdx}
          />
        )}
      </div>
      {uPlotInst && !isSingleOtherSeries && !isIterative && (
        <BarHitsLegend
          uPlotInst={uPlotInst}
          legendDetails={legendDetails}
        />
      )}
    </>
  );
};

export default BarHitsPlot;
