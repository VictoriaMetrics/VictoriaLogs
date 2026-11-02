import { useEffect, useState, useCallback } from "preact/compat";
import { getFromStorage, saveToStorage } from "../../../../utils/storage";
import { toPrefixedKey } from "../../../../utils/storage/utils";
import { GroupLogsType } from "../../../../types";
import useEventListener from "../../../../hooks/useEventListener";

type CollapsedGroups = Set<string>;

const STORAGE_KEY = "GROUP_EXPANSION";
const DEFAULT_COLLAPSED_GROUPS = new Set<string>();
const MAX_ABSENT_COLLAPSED_GROUPS = 100;

export const useGroupExpansion = (groups: GroupLogsType[]) => {
  const [collapsedGroups, setCollapsedGroups] = useState<CollapsedGroups>(DEFAULT_COLLAPSED_GROUPS);
  const allGroupsExpanded = groups.every(group => !collapsedGroups.has(group.keysString));

  const limitCollapsedGroups = (collapsed: Set<string>, groups: GroupLogsType[]): Set<string> => {
    const currentKeys = new Set(groups.map(group => group.keysString));
    const absentKeys = [...collapsed].filter(key => !currentKeys.has(key));
    const retainedAbsentKeys = new Set(absentKeys.slice(-MAX_ABSENT_COLLAPSED_GROUPS));
    return new Set([...collapsed].filter(key => currentKeys.has(key) || retainedAbsentKeys.has(key)),
    );
  };

  const getCollapsedGroupsFromStorage = (): CollapsedGroups => {
    const raw = getFromStorage(STORAGE_KEY, { storage: "session" });

    if (!Array.isArray(raw) || !raw.every(key => typeof key === "string")) {
      return new Set<string>();
    }

    return new Set(raw);
  };

  const saveCollapsedGroupsToStorage = useCallback((next: CollapsedGroups) => {
    const limited = limitCollapsedGroups(next, groups);
    setCollapsedGroups(limited);
    saveToStorage(STORAGE_KEY, [...limited], { storage: "session" });
  }, [groups]);

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
