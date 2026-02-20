import { createPortal, FC, RefObject } from "preact/compat";
import classNames from "classnames";
import "./style.scss";
import { Size } from "../../../hooks/useResizeObserver";
import { useDragResize } from "../../../hooks/useDragResize";

type Props = {
  targetRef: RefObject<HTMLElement>; // element target which size will be changed
  minSize: number;
  dir?: 1 | -1;
  size?: Size;
  onResizeEnd: (width: number) => void;
};

const DragResizeHandle: FC<Props> = ({
  targetRef,
  minSize,
  dir = 1,
  size = {},
  onResizeEnd,
}) => {
  const { isResizing, dragOffset, onMouseDown } = useDragResize({
    targetRef,
    minSize,
    dir,
    axis: "x",
    onResizeEnd,
  });

  const preventNativeDrag = (e: DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const style: Record<string, string | undefined> = {
    height: size.height ? `${size.height}px` : undefined,
    transform: isResizing ? `translateX(${dir * dragOffset}px)` : undefined,
  };

  const handleClickOverlay = (e: MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const DragOverlay = () => (<div
    className="vm-drag-resize-overlay"
    onClick={handleClickOverlay}
  />);

  return (
    <>
      <div
        style={style}
        onMouseDown={onMouseDown}
        onDragStart={preventNativeDrag}
        className={classNames({
          "vm-drag-resize-handle": true,
          "vm-drag-resize-handle_revert": dir === -1,
          "vm-drag-resize-handle_resizing": isResizing,
        })}
      />

      {isResizing && createPortal(<DragOverlay/>, document.body)}
    </>
  );
};

export default DragResizeHandle;
