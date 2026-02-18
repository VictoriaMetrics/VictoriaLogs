import { LogsColumnStats } from "../../Views/TableView/hooks/useTableLogsKeys";
import { ColumnListSortBy } from "./TableSettingsColumns";

export const transparentImg = (() => {
  const img = new Image();
  // 1x1 transparent
  img.src = "data:image/gif;base64,R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==";
  return img;
})();

export const sortColumns = (
  sortBy: ColumnListSortBy,
  stats: Map<string, LogsColumnStats>
) => (a: string, b: string) => {
  switch (sortBy) {
    case "alphabetical":
      return a.localeCompare(b);
    case "frequency": {
      const statA = stats.get(a)?.frequencyPercent ?? 0;
      const statB = stats.get(b)?.frequencyPercent ?? 0;
      return statB - statA;
    }
    case "unique values": {
      const uniqA = stats.get(a)?.uniqValuesCount ?? 0;
      const uniqB = stats.get(b)?.uniqValuesCount ?? 0;
      return uniqB - uniqA;
    }
    default:
      return 0;
  }
};
