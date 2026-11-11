import { LogsFieldValues } from "../../../api/types";

const normalizeSearch = (search: string): string => search.trim().toLowerCase();

const includesSearch = (value: string, search: string): boolean => {
  return value.toLowerCase().includes(search);
};

export const filterSidebarItemsBySearch = (
  values: LogsFieldValues[],
  search: string,
): LogsFieldValues[] => {
  const normalizedSearch = normalizeSearch(search);
  if (!normalizedSearch) return values;

  return values.filter(({ value }) => includesSearch(value, normalizedSearch));
};
