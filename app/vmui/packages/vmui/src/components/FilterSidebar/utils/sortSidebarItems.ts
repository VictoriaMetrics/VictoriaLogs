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

    const nameDiff = a.value.localeCompare(b.value, undefined, { numeric: true });

    if (sort.by === "name") {
      return nameDiff * direction;
    }

    const hitsDiff = (a.hits - b.hits) * direction;
    return hitsDiff || nameDiff;
  });
};
