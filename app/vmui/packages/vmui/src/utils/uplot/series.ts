import uPlot, { Series as uPlotSeries } from "uplot";

export const delSeries = (u: uPlot) => {
  for (let i = u.series.length - 1; i >= 0; i--) {
    i && u.delSeries(i);
  }
};

export const addSeries = (u: uPlot, series: uPlotSeries[], spanGaps = false) => {
  series.forEach((s,i) => {
    if (s.label) s.spanGaps = spanGaps;
    i && u.addSeries(s);
  });
};
