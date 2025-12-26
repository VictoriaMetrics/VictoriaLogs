import uPlot, { Series } from "uplot";
import { LOGS_DEFAULT_BAR_COUNT } from "../../constants/logs";
import { GRAPH_STYLES } from "../../components/Chart/BarHitsChart/types";

const barPaths = (
  u: uPlot,
  seriesIdx: number,
  idx0: number,
  idx1: number,
  barsCount = LOGS_DEFAULT_BAR_COUNT,
): Series.Paths | null => {
  const barSize = (u.under.clientWidth / Math.max(1, barsCount)) - 1;
  const pathBuilderFactory = uPlot?.paths?.bars?.({ size: [0.96, barSize] });
  return pathBuilderFactory ? pathBuilderFactory(u, seriesIdx, idx0, idx1) : null;
};

const lineSteppedPaths = (
  u: uPlot,
  seriesIdx: number,
  idx0: number,
  idx1: number,
): Series.Paths | null => {
  const pathBuilderFactory = uPlot?.paths?.stepped?.({ align: 1 });
  return pathBuilderFactory ? pathBuilderFactory(u, seriesIdx, idx0, idx1) : null;
};

const getSeriesPaths = (type?: GRAPH_STYLES, barsCount = LOGS_DEFAULT_BAR_COUNT) => {
  switch (type) {
    case GRAPH_STYLES.BAR:
      return (u: uPlot, seriesIdx: number, idx0: number, idx1: number) =>
        barPaths(u, seriesIdx, idx0, idx1, barsCount);
    case GRAPH_STYLES.LINE_STEPPED:
      return lineSteppedPaths;
    default:
      return;
  }
};

export default getSeriesPaths;
