import { useCallback, useEffect, useRef, useState } from "preact/compat";
import { type Column, type ColumnKey } from "../types";
import { OrderDir } from "../../../types";
import classNames from "classnames";
import TableSortButton from "./TableSortButton/TableSortButton";
import TableHeaderMenu from "./TableHeaderMenu/TableHeaderMenu";
import Tooltip from "../../Main/Tooltip/Tooltip";
import { ColumnPrefs } from "../hooks/useTableColumnPrefs";
import { Size } from "../../../hooks/useResizeObserver";
import DragResizeHandle from "../../Main/DragResizeHandle/DragResizeHandle";
import { ViewColumnsAction } from "../hooks/useTableColumnView";
import "./style.scss";
import { UseDragColumnReturn } from "../hooks/useDragColumn";

type PropsSort<T> = {
  key: ColumnKey<T>;
  dir: OrderDir;
  onChange: (key: ColumnKey<T>, orderDir: OrderDir) => void;
}

interface Props<T> {
  idx: number;
  column: Column<T>;
  prefs?: ColumnPrefs;
  sort: PropsSort<T>;
  containerSize: Size;
  dragController: UseDragColumnReturn;
  applyViewColumns?: (action: ViewColumnsAction) => void;
  onChangePref: <K extends keyof ColumnPrefs>(colKey: ColumnKey<T>, opsKey: K, value: ColumnPrefs[K]) => void
}

const TableHeaderCell = <T extends object>({
  idx,
  column,
  dragController,
  prefs,
  sort,
  containerSize,
  applyViewColumns,
  onChangePref,
}: Props<T>) => {
  const thRef = useRef<HTMLTableCellElement>(null);
  const titleRef = useRef<HTMLDivElement>(null);

  const { sortable, draggable, resizable, menuEnabled } = column.options || {};

  const title = String(column.title ?? column.key);
  const [isOverflownTitle, setIsOverflownTitle] = useState(false);

  const { draggingKey, insertIndex, isDropping, onDragKeyStart, onDragOverKeyLive, onDragEnd } = dragController;
  const isDragging = draggingKey === column.key;
  const showInsertLeft = idx === 0 && idx === insertIndex;
  const showInsertRight = (idx + 1) === insertIndex;

  const style = prefs?.width
    ? { width: `${prefs?.width}px` }
    : undefined;

  const isSorted = sort.key === column.key;
  const isDesc = sort.dir === "desc";

  const handleClick = useCallback(() => {
    if (sortable) {
      const nextDir: OrderDir = isSorted ? (isDesc ? "asc" : "desc") : "asc";
      sort.onChange(column.key, nextDir);
    }
  }, [column, sort, isSorted, isDesc]);

  const handleResize = useCallback((width: number) => {
    onChangePref(column.key, "width", width);
  }, [column.key]);

  const handleWrapped = useCallback((value: boolean) => {
    onChangePref(column.key, "wrapped", value);
  }, [column.key]);

  const handleHideCol = useCallback(() => {
    if (!applyViewColumns) return;
    applyViewColumns({ type: "set", columnKey: column.key, visible: false });
  }, [applyViewColumns, column.key]);

  const handleDragStart = (e: DragEvent) => {
    if (!draggable) return;
    e.stopPropagation();

    const dt = e.dataTransfer;
    if (!dt) return;

    const thEl = e.currentTarget as HTMLElement;
    onDragKeyStart(column.key, thEl);

    dt.effectAllowed = "move";
    dt.setData("text/plain", column.key);
  };

  const handleDragOver = (e: DragEvent) => {
    if (!draggable) return;
    e.preventDefault();
    const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
    const x = e.clientX - rect.left;
    const pos = x < rect.width / 2 ? "before" : "after";
    onDragOverKeyLive(column.key, pos);
  };

  const handleDrop = (e: DragEvent) => {
    e.preventDefault();
  };


  useEffect(() => {
    const el = titleRef.current;
    if (!el) return;
    setIsOverflownTitle(el.scrollWidth > el.clientWidth);
  }, [title, prefs?.width]);

  const titleEl = (
    <div
      ref={titleRef}
      className="vm-table-cell-header__title"
    >
      {title}
    </div>
  );

  return (
    <th
      ref={thRef}
      draggable={draggable}
      className={classNames({
        "vm-table-cell": true,
        "vm-table-cell-header": true,
        "vm-table-cell-header_sortable": sortable,
        "vm-table-cell-header_draggable": draggable,
        "vm-table-cell-header_dragging": isDragging,
        "vm-table-cell-header_dropping": isDropping,
        "vm-table-cell-header_insert": showInsertLeft || showInsertRight,
        "vm-table-cell-header_insert_left": showInsertLeft,
        "vm-table-cell-header_insert_right": showInsertRight,
        [`${column.classNameHeader}`]: column.classNameHeader
      })}
      style={style}
      onClick={handleClick}
      onDragStart={handleDragStart}
      onDragOver={handleDragOver}
      onDragEnd={onDragEnd}
      onDrop={handleDrop}
    >
      <div
        className="vm-table-cell-header-content"
        style={style}
      >
        {isOverflownTitle ? (<Tooltip title={title}>{titleEl}</Tooltip>) : titleEl}

        {sortable && (
          <TableSortButton
            isSorted={isSorted}
            isDesc={isDesc}
          />
        )}

        {menuEnabled && (
          <TableHeaderMenu
            column={column}
            columnPrefs={prefs}
            onResize={handleResize}
            onWrap={handleWrapped}
            onHideCol={handleHideCol}
          />
        )}

        {resizable && (
          <DragResizeHandle
            targetRef={thRef}
            minSize={80}
            size={containerSize}
            onResizeEnd={handleResize}
          />
        )}
      </div>
    </th>
  );
};

export default TableHeaderCell;
