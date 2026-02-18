import { JSX, ReactNode } from "preact/compat";
import { ViewColumnsAction } from "./hooks/useTableColumnView";
import { OrderDir } from "../../types";

export type ColumnOption = {
  sortable: boolean;
  resizable: boolean;
  draggable: boolean;
  menuEnabled: boolean;
}

export type ColumnKey<T> = Extract<keyof T, string>;

export type Column<T> = {
  title?: string;
  key: ColumnKey<T>;
  className?: string;
  classNameHeader?: string;
  options: ColumnOption,
  render?: (row: T, idx: number) => string | number | JSX.Element;
}

export interface TableProps<T> {
  tableId: string;
  rows: T[];
  columns: Column<T>[];
  defaultOrder?: { key?: ColumnKey<T>, dir?: OrderDir };
  isActiveRow?: (row: T) => boolean;
  onClickRow?: (row: T, e: MouseEvent) => void;
  actionsRender?: (row: T) => ReactNode
  applyViewColumns?: (action: ViewColumnsAction) => void;
  paginationOffset: [number, number];
}
