import { useEffect, useState, useCallback } from "preact/compat";
import { getFromStorage, saveToStorage } from "../../../../utils/storage";
import { toPrefixedKey } from "../../../../utils/storage/utils";
import { GroupLogsType } from "../../../../types";
import useEventListener from "../../../../hooks/useEventListener";

type CollapsedGroups = Set<string>;

const STORAGE_KEY = "GROUP_EXPANSION";
const ALL_COLLAPSED_STORAGE_KEY = "GROUP_ALL_COLLAPSED";
const DEFAULT_COLLAPSED_GROUPS = new Set<string>();
const MAX_ABSENT_COLLAPSED_GROUPS = 100;

const getAllCollapsedState = () => getFromStorage(ALL_COLLAPSED_STORAGE_KEY, { storage: "session" }) === true;

export const useGroupExpansion = (groups: GroupLogsType[]) => {
  const [collapsedGroups, setCollapsedGroups] = useState<CollapsedGroups>(DEFAULT_COLLAPSED_GROUPS);

  const [allCollapsed, setAllCollapsedState] = useState(getAllCollapsedState);

  const allGroupsExpanded = !allCollapsed && groups.every(group => !collapsedGroups.has(group.keysString));

  const limitCollapsedGroups = (collapsed: Set<string>, groups: GroupLogsType[]): Set<string> => {
    const currentKeys = new Set(groups.map(group => group.keysString));
    const absentKeys = [...collapsed].filter(key => !currentKeys.has(key));
    const retainedAbsentKeys = new Set(absentKeys.slice(-MAX_ABSENT_COLLAPSED_GROUPS));
    return new Set([...collapsed].filter(key => currentKeys.has(key) || retainedAbsentKeys.has(key)),
    );
  };

  const setAllCollapsed = useCallback((value: boolean) => {
    setAllCollapsedState(value);
    saveToStorage(ALL_COLLAPSED_STORAGE_KEY, value, { storage: "session" });
  }, []);

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
    if (allGroupsExpanded) {
      setAllCollapsed(true);
    } else {
      setAllCollapsed(false);
      saveCollapsedGroupsToStorage(new Set());
    }
  }, [allGroupsExpanded, setAllCollapsed, saveCollapsedGroupsToStorage]);

  const handleChangeExpand = useCallback((key: string) => (expanded: boolean) => {
    const next = new Set(collapsedGroups);

    if (allCollapsed) {
      groups.forEach(group => next.add(group.keysString));
    }

    expanded ? next.delete(key) : next.add(key);
    saveCollapsedGroupsToStorage(next);
    setAllCollapsed(false);
  }, [collapsedGroups, allCollapsed, groups, saveCollapsedGroupsToStorage, setAllCollapsed]);

  const handleStorageChange = (event: StorageEvent) => {
    if (event.storageArea !== window.sessionStorage) return;

    if (event.key === toPrefixedKey(STORAGE_KEY)) {
      setCollapsedGroups(getCollapsedGroupsFromStorage());
    }

    if (event.key === toPrefixedKey(ALL_COLLAPSED_STORAGE_KEY)) {
      setAllCollapsedState(getFromStorage(ALL_COLLAPSED_STORAGE_KEY, { storage: "session" }) === true);
    }
  };

  useEventListener("storage", handleStorageChange);

  useEffect(() => {
    setCollapsedGroups(getCollapsedGroupsFromStorage());
  }, []);

  return {
    allCollapsed,
    collapsedGroups,
    allGroupsExpanded,
    handleToggleExpandAll,
    handleChangeExpand,
  };
};
