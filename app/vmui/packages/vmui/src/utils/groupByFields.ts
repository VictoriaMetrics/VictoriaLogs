import { Logs } from "../api/types";
import { GROUP_BY_RECENT_LIMIT, NOISE_FIELDS, WITHOUT_GROUPING } from "../constants/logs";
import { getFromStorage, saveToStorage } from "./storage";
import type { TenantUrlType } from "../hooks/useTenant";

const STORAGE_KEY = "LOGS_GROUP_BY_RECENT";

type RecentFieldsStorage = Record<string, string[]>;

const getTenantKey = ({ AccountID, ProjectID }: TenantUrlType) => `${AccountID}:${ProjectID}`;

const isGroupableField = (field: string) => {
  return !!field && field !== WITHOUT_GROUPING && !NOISE_FIELDS.includes(field);
};

const fieldNameCollator = new Intl.Collator();

const sortFields = (fields: string[]) => [...fields].sort(fieldNameCollator.compare);

const getRecentFieldsStorage = (): RecentFieldsStorage => {
  const value = getFromStorage(STORAGE_KEY);
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as RecentFieldsStorage;
};

export const getRecentGroupByFields = (tenant: TenantUrlType): string[] => {
  const fields = getRecentFieldsStorage()[getTenantKey(tenant)];
  if (!Array.isArray(fields)) return [];
  return fields.filter(f => typeof f === "string" && isGroupableField(f)).slice(0, GROUP_BY_RECENT_LIMIT);
};

export const addRecentGroupByField = (tenant: TenantUrlType, field: string): string[] => {
  const fields = getRecentGroupByFields(tenant);
  if (!isGroupableField(field)) return fields;

  const nextFields = [field, ...fields.filter(f => f !== field)].slice(0, GROUP_BY_RECENT_LIMIT);
  saveToStorage(STORAGE_KEY, {
    ...getRecentFieldsStorage(),
    [getTenantKey(tenant)]: nextFields,
  });
  return nextFields;
};

export const getUniqueFieldNames = (logs: Logs[]): string[] => {
  const fields = new Set<string>();
  logs.forEach(log => Object.keys(log).forEach(key => fields.add(key)));
  return sortFields(Array.from(fields));
};

interface GroupByOptionsSources {
  current: string;
  recent: string[];
  loaded: string[];
  fetched: string[];
}

export const buildGroupByOptions = ({ current, recent, loaded, fetched }: GroupByOptionsSources): string[] => {
  const top = [current, ...recent].filter(isGroupableField);
  const rest = sortFields([...loaded, ...fetched].filter(isGroupableField));
  return Array.from(new Set([WITHOUT_GROUPING, ...top, ...rest]));
};
