import { getFromStorage, saveToStorage } from "../../utils/storage";
import { MAX_QUERIES_HISTORY, MAX_QUERY_FIELDS } from "../../constants/logs";
import { HistoryStorage, QueryHistoryEntry, QueryHistoryGroup } from "./types";
import { vmDate } from "../../utils/time";
import { HISTORY_DATE_FORMAT } from "../../constants/date";
import { DEFAULT_QUERY } from "../../pages/QueryPage/hooks/useQueryController";

const STORAGE_KEY = "LOGS_QUERY_HISTORY";
const DEFAULT_HISTORY_TYPE = "QUERY_HISTORY";

const getHistoryStorage = (): HistoryStorage => {
  try {
    const list = getFromStorage(STORAGE_KEY) as string;
    return JSON.parse(list);
  } catch (e) {
    return {};
  }
};

export const getHistoryFromStorage = (): QueryHistoryEntry[] => {
  const history = getHistoryStorage();
  const values = history[DEFAULT_HISTORY_TYPE]?.[0] || [];
  const meta = history.QUERY_HISTORY_META || {};

  return values.map(query => ({
    query,
    lastRunAt: meta[query],
  }));
};

export const addQueryToHistoryStorage = (query: string) => {
  const nextQuery = query.trim();
  if (!nextQuery || nextQuery === DEFAULT_QUERY) return;

  const storageHistory = getHistoryStorage();
  const storageValues = storageHistory[DEFAULT_HISTORY_TYPE] || [];
  const values = storageValues[0] || [];
  const totalLimit = MAX_QUERIES_HISTORY * MAX_QUERY_FIELDS;

  const nextValues = [
    nextQuery,
    ...values.filter(v => v !== nextQuery),
  ].slice(0, totalLimit);

  const now = Date.now();
  const prevMeta = storageHistory.QUERY_HISTORY_META || {};

  const nextMeta = Object.fromEntries(
    nextValues
      .filter(v => v === nextQuery || prevMeta[v] !== undefined)
      .map(v => [v, v === nextQuery ? now : prevMeta[v]])
  );

  saveToStorage(STORAGE_KEY, JSON.stringify({
    ...storageHistory,
    [DEFAULT_HISTORY_TYPE]: [nextValues],
    QUERY_HISTORY_META: nextMeta,
  }));
};

export const removeQueryFromHistoryStorage = (query: string) => {
  const storageHistory = getHistoryStorage();
  const values = storageHistory[DEFAULT_HISTORY_TYPE]?.[0] || [];
  const nextValues = values.filter(v => v !== query);

  const prevMeta = storageHistory.QUERY_HISTORY_META || {};
  const nextMeta = Object.fromEntries(
    nextValues
      .filter(v => prevMeta[v] !== undefined)
      .map(v => [v, prevMeta[v]])
  );

  saveToStorage(STORAGE_KEY, JSON.stringify({
    ...storageHistory,
    [DEFAULT_HISTORY_TYPE]: [nextValues],
    QUERY_HISTORY_META: nextMeta,
  }));
};

export const clearQueryHistoryStorage = () => {
  const history = getHistoryStorage();

  saveToStorage(STORAGE_KEY, JSON.stringify({
    ...history,
    [DEFAULT_HISTORY_TYPE]: [],
    QUERY_HISTORY_META: {},
  }));
};

export const formatHistoryDate = (timestamp?: number) => {
  if (!timestamp) return "";
  return vmDate(timestamp).format("HH:mm");
};

const getHistoryGroupTitle = (entry: QueryHistoryEntry) => {
  if (!entry.lastRunAt) return "Earlier";

  const date = vmDate(entry.lastRunAt);
  const now = vmDate();

  if (date.isSame(now, "day")) return `Today - ${date.format(HISTORY_DATE_FORMAT)}`;
  if (date.isSame(now.subtract(1, "day"), "day")) return `Yesterday - ${date.format(HISTORY_DATE_FORMAT)}`;

  return date.format(HISTORY_DATE_FORMAT);
};

export const groupHistoryByDay = (entries: QueryHistoryEntry[]): QueryHistoryGroup[] => {
  const groups = new Map<string, QueryHistoryEntry[]>();

  const sortedEntries = entries.toSorted((a, b) => {
    const aDate = a.lastRunAt || 0;
    const bDate = b.lastRunAt || 0;

    return bDate - aDate;
  });

  sortedEntries.forEach(entry => {
    const title = getHistoryGroupTitle(entry);
    const group = groups.get(title) || [];
    group.push(entry);
    groups.set(title, group);
  });

  return Array.from(groups, ([title, values]) => ({
    title,
    entries: values,
  }));
};
