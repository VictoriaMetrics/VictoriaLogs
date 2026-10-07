import { LogsFieldValues } from "../../../api/types";

const normalizeSearch = (search: string): string => search.trim().toLowerCase();

const includesSearch = (value: string, search: string): boolean => {
  return value.toLowerCase().includes(search);
};

export const matchesFilterSidebarSearch = (
  fieldName: string,
  values: LogsFieldValues[],
  search: string,
): boolean => {
  const normalizedSearch = normalizeSearch(search);
  if (!normalizedSearch || includesSearch(fieldName, normalizedSearch)) return true;

  return values.some(({ value }) => includesSearch(value, normalizedSearch));
};

export const filterFilterSidebarValues = (
  values: LogsFieldValues[],
  fieldName: string,
  search: string,
): LogsFieldValues[] => {
  const normalizedSearch = normalizeSearch(search);
  if (!normalizedSearch || includesSearch(fieldName, normalizedSearch)) return values;

  return values.filter(({ value }) => includesSearch(value, normalizedSearch));
};
