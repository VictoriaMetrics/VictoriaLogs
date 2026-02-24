import uPlot from "uplot";
import { SetMinMax } from "../../types";

const MIN_SELECT_PX = 24;

export const setSelect = (setPlotScale: SetMinMax) => (u: uPlot) => {
  const min = u.posToVal(u.select.left, "x");
  const max = u.posToVal(u.select.left + u.select.width, "x");
  if (u.select.width < MIN_SELECT_PX) return;
  setPlotScale({ min, max });
};
