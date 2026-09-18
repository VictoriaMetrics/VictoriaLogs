import { RefObject, useCallback, useRef } from "preact/compat";
import useEventListener from "../useEventListener";
import { MinMax, SetMinMax } from "../../types";

interface ZoomChartHook {
  uPlotInst?: uPlot;
  element: RefObject<HTMLDivElement>
  xRange: MinMax;
  setPlotScale: SetMinMax;
}

const calculateDistance = (touches: TouchList) => {
  const dx = touches[0].clientX - touches[1].clientX;
  const dy = touches[0].clientY - touches[1].clientY;
  return Math.sqrt(dx * dx + dy * dy);
};

const minTouchDistanceDiff = 1;

const useZoomChart = ({ uPlotInst, element, xRange, setPlotScale }: ZoomChartHook) => {
  const previousTouchDistance = useRef(0);

  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    const { target, ctrlKey, metaKey, key } = e;
    const isInput = target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement;
    if (!uPlotInst || isInput) return;

    const isPlus = key === "+" || key === "=";
    const isMinus = key === "-";
    const isNotControlKey = !(ctrlKey || metaKey);

    if ((isMinus || isPlus) && isNotControlKey) {
      e.preventDefault();
      const factor = (xRange.max - xRange.min) / 10 * (isPlus ? 1 : -1);
      setPlotScale({ min: xRange.min + factor, max: xRange.max - factor });
    }
  }, [uPlotInst, xRange]);

  const handleTouchStart = (e: TouchEvent) => {
    if (e.touches.length === 2) {
      e.preventDefault();
      previousTouchDistance.current = calculateDistance(e.touches);
    }
  };

  const handleTouchMove = useCallback((e: TouchEvent) => {
    if (!uPlotInst || e.touches.length !== 2) return;
    e.preventDefault();

    const currentTouchDistance = calculateDistance(e.touches);
    const previousDistance = previousTouchDistance.current;
    if (currentTouchDistance <= 0) return;
    if (previousDistance <= 0) {
      previousTouchDistance.current = currentTouchDistance;
      return;
    }

    const diffDistance = previousDistance - currentTouchDistance;
    if (Math.abs(diffDistance) < minTouchDistanceDiff) return;

    const max = (uPlotInst.scales.x.max || xRange.max);
    const min = (uPlotInst.scales.x.min || xRange.min);
    const dur = max - min;
    const dir = (diffDistance > 0 ? -1 : 1);

    const zoomFactor = dur / 50 * dir;
    const nextDuration = dur - (zoomFactor * 2);
    const touchCenter = (e.touches[0].clientX + e.touches[1].clientX) / 2;
    const { left, width } = uPlotInst.over.getBoundingClientRect();
    if (width <= 0) return;

    previousTouchDistance.current = currentTouchDistance;

    const zoomPosition = Math.min(width, Math.max(0, touchCenter - left));
    const anchorRatio = zoomPosition / width;
    const anchorValue = uPlotInst.posToVal(zoomPosition, "x");

    uPlotInst.batch(() => setPlotScale({
      min: anchorValue - (anchorRatio * nextDuration),
      max: anchorValue + ((1 - anchorRatio) * nextDuration),
    }));
  }, [uPlotInst, xRange]);

  useEventListener("keydown", handleKeyDown);
  useEventListener("touchmove", handleTouchMove, element, { passive: false });
  useEventListener("touchstart", handleTouchStart, element);

  return null;
};

export default useZoomChart;
