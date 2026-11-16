import { useRef } from "preact/compat";
import uPlot from "uplot";
import { SetMinMax } from "../../types";

interface DragHookArgs {
  dragSpeed: number,
  setPanning: (enable: boolean) => void,
  setPlotScale: SetMinMax
}

interface DragArgs {
  e: MouseEvent | TouchEvent,
  u: uPlot,
}

const isMouseEvent = (e: MouseEvent | TouchEvent): e is MouseEvent => e instanceof MouseEvent;
const getClientX = (e: MouseEvent | TouchEvent) => isMouseEvent(e) ? e.clientX : e.touches[0].clientX;
const getClientY = (e: MouseEvent | TouchEvent) => isMouseEvent(e) ? e.clientY : e.touches[0].clientY;

const touchDragThreshold = 8;

const useDragChart = ({ dragSpeed = 0.85, setPanning, setPlotScale }: DragHookArgs) => {
  const dragState = useRef({
    leftStart: 0,
    topStart: 0,
    xUnitsPerPx: 0,
    scXMin: 0,
    scXMax: 0,
    isTouch: false,
    isPanning: false,
  });

  const mouseMove = (e: MouseEvent | TouchEvent) => {
    if (!isMouseEvent(e) && e.touches.length !== 1) return;

    const clientX = getClientX(e);
    const clientY = getClientY(e);
    const { leftStart, topStart, xUnitsPerPx, scXMin, scXMax, isTouch } = dragState.current;
    const diffX = clientX - leftStart;

    if (isTouch && !dragState.current.isPanning) {
      const diffY = clientY - topStart;
      const absX = Math.abs(diffX);
      const absY = Math.abs(diffY);

      if (absX < touchDragThreshold && absY < touchDragThreshold) return;

      if (absY >= absX) {
        mouseUp();
        return;
      }

      dragState.current.isPanning = true;
      setPanning(true);
    }

    e.preventDefault();
    const dx = xUnitsPerPx * ((clientX - leftStart) * dragSpeed);
    setPlotScale({ min: scXMin - dx, max: scXMax - dx });
  };

  const mouseUp = () => {
    setPanning(false);
    document.removeEventListener("mousemove", mouseMove);
    document.removeEventListener("mouseup", mouseUp);
    document.removeEventListener("touchmove", mouseMove);
    document.removeEventListener("touchend", mouseUp);
    document.removeEventListener("touchcancel", mouseUp);
  };

  const mouseDown = () => {
    document.addEventListener("mousemove", mouseMove);
    document.addEventListener("mouseup", mouseUp);
    document.addEventListener("touchmove", mouseMove, { passive: false });
    document.addEventListener("touchend", mouseUp);
    document.addEventListener("touchcancel", mouseUp);
  };

  return ({ e, u }: DragArgs): void => {
    const isTouch = !isMouseEvent(e);

    if (!isTouch) {
      e.preventDefault();
      setPanning(true);
    }

    dragState.current = {
      leftStart: getClientX(e),
      topStart: getClientY(e),
      xUnitsPerPx: u.posToVal(1, "x") - u.posToVal(0, "x"),
      scXMin: u.scales.x.min || 0,
      scXMax: u.scales.x.max || 0,
      isTouch,
      isPanning: !isTouch,
    };

    mouseDown();
  };
};

export default useDragChart;
