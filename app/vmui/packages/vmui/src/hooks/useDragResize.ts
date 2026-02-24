import { useEffect, useRef, useState } from "preact/hooks";
import { RefObject } from "preact/compat";
import { borderBoxToContentSize } from "../utils/dom-geometry";

type Axis = "x" | "y";
type Direction = 1 | -1;

type Options = {
  targetRef: RefObject<HTMLElement>;
  minSize?: number;
  axis?: Axis;
  dir?: Direction;
  onResizeEnd: (sizePx: number) => void;
};

export function useDragResize({
  targetRef,
  minSize = 80,
  axis = "x",
  dir = 1,
  onResizeEnd,
}: Options) {
  const [isResizing, setIsResizing] = useState(false);
  const [dragOffset, setDragOffset] = useState(0);

  const startPosRef = useRef(0);
  const startSizeRef = useRef(0);
  const dragOffsetRef = useRef(0);

  const cleanupRef = useRef<null | (() => void)>(null);

  const getClientPos = (e: MouseEvent) => (axis === "x" ? e.clientX : e.clientY);

  const getCurrentSize = (el: HTMLElement) => {
    const rect = el.getBoundingClientRect();
    return axis === "x" ? rect.width : rect.height;
  };

  const onMouseDown = (mouseDownEvent: MouseEvent) => {
    cleanupRef.current?.();

    mouseDownEvent.preventDefault();
    mouseDownEvent.stopPropagation();

    const target = targetRef.current;
    if (!target) return;

    setIsResizing(true);
    setDragOffset(0);
    dragOffsetRef.current = 0;

    startPosRef.current = getClientPos(mouseDownEvent);
    startSizeRef.current = getCurrentSize(target);

    const onMouseMove = (mouseMoveEvent: MouseEvent) => {
      const rawOffset = (getClientPos(mouseMoveEvent) - startPosRef.current) * dir;
      const minOffset = minSize - startSizeRef.current;
      const nextOffset = Math.max(rawOffset, minOffset);

      dragOffsetRef.current = nextOffset;
      setDragOffset(nextOffset);
    };

    const onMouseUp = () => {
      const finalBorderBox = Math.max(minSize, startSizeRef.current + dragOffsetRef.current);
      const finalSize = borderBoxToContentSize(target, finalBorderBox, axis);

      onResizeEnd(finalSize);

      setIsResizing(false);
      setDragOffset(0);
      dragOffsetRef.current = 0;

      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);

      cleanupRef.current = null;
    };

    cleanupRef.current = () => {
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
    };

    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
  };

  useEffect(() => {
    return () => {
      cleanupRef.current?.();
      cleanupRef.current = null;
    };
  }, []);

  return {
    isResizing,
    dragOffset,
    onMouseDown
  };
}
