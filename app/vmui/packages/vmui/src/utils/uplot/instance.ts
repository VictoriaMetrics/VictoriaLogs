import uPlot from "uplot";
import { delSeries } from "./series";
import { delHooks } from "./hooks";

export const handleDestroy = (u: uPlot) => {
  delSeries(u);
  delHooks(u);
  u.setData([]);
};
