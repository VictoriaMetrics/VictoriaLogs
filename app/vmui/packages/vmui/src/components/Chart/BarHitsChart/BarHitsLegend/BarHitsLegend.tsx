import { FC, useEffect, useState } from "preact/compat";
import uPlot, { Series } from "uplot";
import "./style.scss";
import BarHitsLegendItem from "./BarHitsLegendItem";
import { LegendLogHits } from "../../../../api/types";

interface Props {
  uPlotInst: uPlot;
  legendDetails: LegendLogHits[];
  onApplyFilter: (value: string) => void;
}

const BarHitsLegend: FC<Props> = ({ uPlotInst, legendDetails, onApplyFilter }) => {
  const [series, setSeries] = useState<Series[]>([]);

  const getSeries = () => {
    return uPlotInst.series.filter(s => s.scale !== "x");
  };

  const handleRedrawGraph = () => {
    uPlotInst.redraw();
  };

  useEffect(() => {
    if (!uPlotInst.hooks.draw) {
      uPlotInst.hooks.draw = [];
    }
    uPlotInst.hooks.draw.push(() => {
      setSeries(getSeries());
    });
  }, [uPlotInst]);

  return (
    <div className="vm-bar-hits-legend">
      {legendDetails.map((legend) => (
        <BarHitsLegendItem
          key={legend.label}
          legend={legend}
          series={series}
          onRedrawGraph={handleRedrawGraph}
          onApplyFilter={onApplyFilter}
        />
      ))}
    </div>
  );
};

export default BarHitsLegend;
