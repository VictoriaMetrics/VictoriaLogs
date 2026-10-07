import { FC, useEffect } from "preact/compat";
import uPlot from "uplot";
import "./style.scss";

type BarRect = {
  x: number;
  y: number;
  width: number;
  height: number
}

type Props = {
  uPlotInst: uPlot;
  getLoadingBarRect: (u: uPlot) => BarRect | null
}

const BarHitsLoadingOverlay: FC<Props> = ({ uPlotInst, getLoadingBarRect }) => {
  useEffect(() => {
    const overlay = document.createElement("div");
    overlay.className = "vm-bar-hits-loading-overlay";
    uPlotInst.over.appendChild(overlay);

    const handleDraw = (u: uPlot) => {
      const rect = getLoadingBarRect(u);

      overlay.style.display = rect ? "block" : "none";
      if (!rect) return;

      overlay.style.left = `${rect.x}px`;
      overlay.style.top = `${rect.y}px`;
      overlay.style.width = `${rect.width}px`;
    };

    (uPlotInst.hooks.draw ??= []).push(handleDraw);
    handleDraw(uPlotInst);

    return () => {
      overlay.remove();

      const hooks = uPlotInst.hooks.draw || [];
      const index = hooks.indexOf(handleDraw);
      if (index !== -1) hooks.splice(index, 1);
    };
  }, [uPlotInst, getLoadingBarRect]);

  return null;
};

export default BarHitsLoadingOverlay;
