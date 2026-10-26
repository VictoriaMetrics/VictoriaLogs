import { useEffect, useState, useCallback } from "preact/compat";
import { getFromStorage, saveToStorage } from "../../../../utils/storage";
import { toPrefixedKey } from "../../../../utils/storage/utils";
import { GroupLogsType } from "../../../../types";
import useEventListener from "../../../../hooks/useEventListener";

type CollapsedGroups = Set<string>;

const STORAGE_KEY = "GROUP_EXPANSION";
const DEFAULT_COLLAPSED_GROUPS = new Set<string>();

export const useGroupExpansion = (groups: GroupLogsType[]) => {
  const [collapsedGroups, setCollapsedGroups] = useState<CollapsedGroups>(DEFAULT_COLLAPSED_GROUPS);
  const allGroupsExpanded = groups.every(group => !collapsedGroups.has(group.keysString));

  const getCollapsedGroupsFromStorage = (): CollapsedGroups => {
    const raw = getFromStorage(STORAGE_KEY, { storage: "session" });

    if (!Array.isArray(raw) || !raw.every(key => typeof key === "string")) {
      return new Set<string>();
    }

    return new Set(raw);
  };

  const saveCollapsedGroupsToStorage = useCallback((next: CollapsedGroups) => {
    saveToStorage(STORAGE_KEY, Array.from(next), { storage: "session" });
  }, []);

  const handleToggleExpandAll = useCallback(() => {
    const next = new Set(collapsedGroups);

    for (const group of groups) {
      allGroupsExpanded ? next.add(group.keysString) : next.delete(group.keysString);
    }

    saveCollapsedGroupsToStorage(next);
  }, [groups, collapsedGroups, allGroupsExpanded, saveCollapsedGroupsToStorage]);

  const handleChangeExpand = useCallback((key: string) => (expanded: boolean) => {
    const next = new Set(collapsedGroups);
    expanded ? next.delete(key) : next.add(key);
    saveCollapsedGroupsToStorage(next);
  }, [collapsedGroups, saveCollapsedGroupsToStorage]);

  const handleStorageChange = (event: StorageEvent) => {
    if (event?.storageArea !== window.sessionStorage || event?.key !== toPrefixedKey(STORAGE_KEY)) return;
    setCollapsedGroups(getCollapsedGroupsFromStorage());
  };

  useEventListener("storage", handleStorageChange);

  useEffect(() => {
    setCollapsedGroups(getCollapsedGroupsFromStorage());
  }, []);

  return {
    collapsedGroups,
    allGroupsExpanded,
    handleToggleExpandAll,
    handleChangeExpand,
  };
};
