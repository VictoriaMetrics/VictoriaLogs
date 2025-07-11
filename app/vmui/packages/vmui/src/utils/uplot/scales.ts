import uPlot from "uplot";
import { SetMinMax } from "../../types";

export const setSelect = (setPlotScale: SetMinMax) => (u: uPlot) => {
  const min = u.posToVal(u.select.left, "x");
  const max = u.posToVal(u.select.left + u.select.width, "x");
  setPlotScale({ min, max });
};
