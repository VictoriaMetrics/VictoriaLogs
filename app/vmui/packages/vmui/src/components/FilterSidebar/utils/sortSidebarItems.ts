import { LogsFieldValues } from "../../../api/types";
import { SortOptions } from "../types";

export const sortSidebarItems = (
  items: LogsFieldValues[],
  sort: SortOptions,
  selectedItems?: ReadonlySet<string>,
): LogsFieldValues[] => {
  const direction = sort.direction === "asc" ? 1 : -1;

  return items.toSorted((a, b) => {
    const selectedDiff = Number(selectedItems?.has(b.value)) - Number(selectedItems?.has(a.value));

    if (selectedDiff) return selectedDiff;

    if (sort.by === "name") {
      return nameComparator(a.value, b.value) * direction;
    }

    const hitsDiff = (a.hits - b.hits) * direction;
    return hitsDiff || nameComparator(a.value, b.value);
  });
};

const nameComparator = (a: string, b: string): number => {
  return a.localeCompare(b, undefined, { numeric: true });
};
