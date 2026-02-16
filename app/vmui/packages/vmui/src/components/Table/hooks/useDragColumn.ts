import { useMemo, useRef, useState } from "preact/compat";
import { arrayMove } from "../../../utils/array";

type Options = {
  arr: string[];
  axis: "x" | "y";
  onChange: (a: string[]) => void
}

export type UseDragColumnReturn = {
  draggingKey: string | null;
  itemSize: number;
  isDropping: boolean;
  insertIndex: number | null;
  getShiftUnits: (idx: number) => number;
  onDragKeyStart: (key: string, rowEl?: HTMLElement | null) => void;
  onDragOverKeyLive: (overKey: string, pos: "before" | "after" | null) => void;
  onDragEnd: () => void;
}

export const useDragColumn = ({ arr, axis, onChange }: Options): UseDragColumnReturn => {
  const [draggingKey, setDraggingKey] = useState<string | null>(null);
  const [fromIndex, setFromIndex] = useState<number | null>(null);
  const [toIndex, setToIndex] = useState<number | null>(null);
  const [itemSize, setItemSize] = useState<number>(0);
  const [isDropping, setIsDropping] = useState(false);
  const [insertIndex, setInsertIndex] = useState<number | null>(null);

  const insertRef = useRef<number | null>(null);
  const fromRef = useRef<number | null>(null);
  const toRef = useRef<number | null>(null);
  const dragKeyRef = useRef<string | null>(null);

  const indexMap = useMemo(() => {
    const m = new Map<string, number>();
    arr.forEach((k, i) => m.set(k, i));
    return m;
  }, [arr]);

  const onDragKeyStart = (key: string, rowEl?: HTMLElement | null) => {
    dragKeyRef.current = key;
    const from = indexMap.get(key) ?? null;
    setDraggingKey(key);
    setFromIndex(from);
    setToIndex(from);
    setInsertIndex(from);

    insertRef.current = from;
    fromRef.current = from;
    toRef.current = from;

    const h = rowEl?.offsetHeight ?? 0;
    const w = rowEl?.offsetWidth ?? 0;
    const size = axis === "x" ? w : h;
    if (size > 0) setItemSize(size);
  };

  const onDragOverKeyLive = (overKey: string, pos: "before" | "after" | null) => {
    const dragKey = dragKeyRef.current;
    if (!dragKey || pos == null) return;
    if (overKey === dragKey) return;

    const from = indexMap.get(dragKey);
    const overIndex = indexMap.get(overKey);
    if (from == null || overIndex == null) return;

    let insert = pos === "before" ? overIndex : overIndex + 1;
    if (insert < 0) insert = 0;
    if (insert > arr.length) insert = arr.length;

    insertRef.current = insert;
    setInsertIndex(prev => (prev === insert ? prev : insert));

    let to = insert;
    if (from < to) to -= 1;
    if (to < 0) to = 0;
    if (to > arr.length - 1) to = arr.length - 1;

    toRef.current = to;
    setToIndex(prev => (prev === to ? prev : to));
  };

  const onDragEnd = () => {
    const dragKey = dragKeyRef.current;
    const from = fromRef.current;
    const to = toRef.current;

    fromRef.current = null;
    toRef.current = null;
    dragKeyRef.current = null;

    setIsDropping(true);

    if (dragKey && from != null && to != null && from !== to) {
      onChange(arrayMove(arr, from, to));
    }

    requestAnimationFrame(() => {
      setDraggingKey(null);
      setFromIndex(null);
      setToIndex(null);
      setInsertIndex(null);
      insertRef.current = null;

      requestAnimationFrame(() => {
        setIsDropping(false);
      });
    });
  };

  const getShiftUnits = (idx: number) => {
    if (fromIndex == null || toIndex == null) return 0;

    if (idx === fromIndex) return toIndex - fromIndex;

    if (toIndex > fromIndex) {
      if (idx > fromIndex && idx <= toIndex) return -1;
    } else if (toIndex < fromIndex) {
      if (idx >= toIndex && idx < fromIndex) return 1;
    }
    return 0;
  };

  return {
    draggingKey,
    itemSize,
    isDropping,
    insertIndex,
    getShiftUnits,
    onDragKeyStart,
    onDragOverKeyLive,
    onDragEnd
  };
};
