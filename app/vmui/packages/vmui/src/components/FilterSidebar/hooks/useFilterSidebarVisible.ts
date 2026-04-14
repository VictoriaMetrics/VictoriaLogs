import { useEffect, useState } from "preact/compat";
import { getFromStorage, removeFromStorage, saveToStorage } from "../../../utils/storage";
import useEventListener from "../../../hooks/useEventListener";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import { toPrefixedKey } from "../../../utils/storage/utils";

const HIDDEN_STORAGE_KEY = "LOGS_FILTER_SIDEBAR_HIDDEN";

export const useFilterSidebarVisible = () => {
  const { isMobile } = useDeviceDetect();
  const storageValue = getFromStorage(HIDDEN_STORAGE_KEY) === "true";
  const [isHidden, setIsHidden] = useState(isMobile ? true : storageValue);

  const setVisible = (isVisible: boolean) => {
    setIsHidden(!isVisible);
    if (isVisible) removeFromStorage([HIDDEN_STORAGE_KEY]);
    else saveToStorage(HIDDEN_STORAGE_KEY, "true");
  };

  const setHidden = () => setVisible(false);

  const updateState = (e?: StorageEvent) => {
    if (e && e.key !== toPrefixedKey(HIDDEN_STORAGE_KEY)) return;
    setIsHidden(getFromStorage(HIDDEN_STORAGE_KEY) === "true");
  };

  useEventListener("storage", updateState);
  useEffect(() => {
    if (!isMobile) updateState();
  }, []);

  return {
    isVisible: !isHidden,
    setVisible,
    setHidden,
  };
};
