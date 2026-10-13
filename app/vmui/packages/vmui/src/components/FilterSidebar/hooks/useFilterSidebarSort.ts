import { useCallback, useState } from "preact/compat";
import useEventListener from "../../../hooks/useEventListener";
import { getFromStorage, saveToStorage } from "../../../utils/storage";
import { toPrefixedKey } from "../../../utils/storage/utils";
import { SortOptions } from "../types";

const SORT_STORAGE_KEY = "LOGS_FILTER_SIDEBAR_SORT";

export const DEFAULT_FILTER_SIDEBAR_SORT: SortOptions = {
  by: "hits",
  direction: "desc",
};

const isSortOptions = (value: unknown): value is SortOptions => {
  if (!value || typeof value !== "object") return false;

  const sort = value as Partial<SortOptions>;
  const isValidSortBy = sort.by === "hits" || sort.by === "name";
  const isValidDirection = sort.direction === "asc" || sort.direction === "desc";

  return isValidSortBy && isValidDirection;
};

const getStoredSort = (): SortOptions => {
  const storedSort = getFromStorage(SORT_STORAGE_KEY);
  return isSortOptions(storedSort) ? storedSort : DEFAULT_FILTER_SIDEBAR_SORT;
};

export const useFilterSidebarSort = () => {
  const [sort, setSortState] = useState<SortOptions>(getStoredSort);

  const setSort = useCallback((nextSort: SortOptions) => {
    setSortState(nextSort);
    saveToStorage(SORT_STORAGE_KEY, nextSort);
  }, []);

  const updateSort = useCallback((event: StorageEvent) => {
    if (event.key !== toPrefixedKey(SORT_STORAGE_KEY)) return;

    const nextSort = getStoredSort();
    setSortState(currentSort => {
      const isSameBy = currentSort.by === nextSort.by;
      const isSameDir = currentSort.direction === nextSort.direction;
      const isSameSort = isSameBy && isSameDir;
      return isSameSort ? currentSort : nextSort;
    });
  }, []);

  useEventListener("storage", updateSort);

  return {
    sort,
    setSort,
  };
};
