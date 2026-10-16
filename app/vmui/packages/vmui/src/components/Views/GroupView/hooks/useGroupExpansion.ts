import { getFromStorage, saveToStorage } from "../../../../utils/storage";
import { useEffect, useMemo, useState } from "preact/compat";
import useEventListener from "../../../../hooks/useEventListener";
import { toPrefixedKey } from "../../../../utils/storage/utils";
import { useCallback } from "react";
import { GroupLogsType } from "../../../../types";

type ExpandedByGroup = Map<string, boolean>

const STORAGE_KEY = "GROUP_EXPANSION";
const DEFAULT_EXPANDED_BY_GROUP = new Map<string, boolean>();

export const useGroupExpansion = () => {
  const [expandedByGroup, setExpandedByGroup] = useState<ExpandedByGroup>(DEFAULT_EXPANDED_BY_GROUP);
  const allGroupsExpanded = useMemo(() => [...expandedByGroup.values()].every(Boolean), [expandedByGroup]);

  const getExpandedByGroupFromStorage = (): ExpandedByGroup => {
    const raw = getFromStorage(STORAGE_KEY, { storage: "session" });

    if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
      return DEFAULT_EXPANDED_BY_GROUP;
    }

    const entries = Object.entries(raw);
    if (!entries.every(([, value]) => typeof value === "boolean")) {
      return DEFAULT_EXPANDED_BY_GROUP;
    }

    return new Map(entries as [string, boolean][]);
  };

  const saveExpandedByGroupToStorage = useCallback((next: ExpandedByGroup) => {
    saveToStorage(STORAGE_KEY, Object.fromEntries(next), { storage: "session" });
  }, []);

  const handleToggleExpandAll = useCallback((groups: GroupLogsType[]) => {
    const next = new Map(groups.map(g => [g.keysString, !allGroupsExpanded]));
    saveExpandedByGroupToStorage(next);
  }, [allGroupsExpanded, saveExpandedByGroupToStorage]);

  const handleChangeExpand = useCallback((key: string) => (value: boolean) => {
    const next = new Map(expandedByGroup);
    next.set(key, value);
    saveExpandedByGroupToStorage(next);
  }, [expandedByGroup, saveExpandedByGroupToStorage]);

  const syncExpandedByGroup = useCallback((groups: GroupLogsType[], defaultExpanded: boolean) => {
    setExpandedByGroup(prev => {
      const allCollapsed = prev.size > 0 && [...prev.values()].every(value => !value);

      return new Map(groups.map(g => [
        g.keysString,
        prev.get(g.keysString) ?? (allCollapsed ? false : defaultExpanded),
      ]));
    });
  }, []);

  const handleStorageChange = (event: StorageEvent) => {
    if (event?.storageArea !== window.sessionStorage || event?.key !== toPrefixedKey(STORAGE_KEY)) return;
    const stored = getExpandedByGroupFromStorage();

    // Apply stored expansion states only to current groups.
    setExpandedByGroup(prev => new Map(
      [...prev].map(([key, expanded]) => [
        key,
        stored.get(key) ?? expanded,
      ])
    ));
  };

  useEventListener("storage", handleStorageChange);

  useEffect(() => {
    setExpandedByGroup(getExpandedByGroupFromStorage());
  }, []);

  return {
    expandedByGroup,
    allGroupsExpanded,
    handleToggleExpandAll,
    handleChangeExpand,
    syncExpandedByGroup,
  };
};
