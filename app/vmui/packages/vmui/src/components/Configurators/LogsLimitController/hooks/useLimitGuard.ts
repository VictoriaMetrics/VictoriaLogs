import { useCallback, useEffect, useRef, useState } from "preact/compat";
import {
  LOGS_CONFIRM_THRESHOLD,
  LOGS_MAX_LIMIT,
  LOGS_LIMIT_WARN_DISMISSED_KEY,
} from "../../../../constants/logs";
import { BeforeFetch, BeforeFetchResult } from "../../../../pages/QueryPage/hooks/useFetchLogs";
import useBoolean from "../../../../hooks/useBoolean";

type WarningPreference = {
  suppress: boolean;
  permanent: boolean;
};

const getStoredWarningPreference = (): WarningPreference => {
  const read = (storage: Storage): boolean => {
    try {
      return Boolean(storage.getItem(LOGS_LIMIT_WARN_DISMISSED_KEY));
    } catch {
      return false;
    }
  };

  const permanent = read(localStorage);
  const session = read(sessionStorage);

  return {
    suppress: permanent || session,
    permanent,
  };
};

type Params = {
  setLimit: (value: number) => void;
};

export const useLimitGuard = ({ setLimit }: Params) => {
  const { value: isOpen, setFalse: handleClose, setTrue: handleOpen } = useBoolean(false);

  const [initialLimit, setInitialLimit] = useState<number>(0);
  const [limitDraft, setLimitDraft] = useState<number>(0);

  const [warningPreference, setWarningPreference] = useState<WarningPreference>(getStoredWarningPreference);
  const { suppress: suppressWarning, permanent: persistWarning } = warningPreference;

  const pendingResolveRef = useRef<(r: BeforeFetchResult) => void>();
  const pendingPromiseRef = useRef<Promise<BeforeFetchResult> | null>(null);

  const beforeFetch: BeforeFetch = useCallback(async (body) => {
    if (pendingPromiseRef.current) return pendingPromiseRef.current;

    const n = Number(body.get("limit") ?? 0);
    const safeLimit = Number.isFinite(n) && n >= 0 ? n : 0;

    const mustConfirm = safeLimit === 0 || safeLimit > LOGS_MAX_LIMIT;
    const softConfirm = safeLimit > LOGS_CONFIRM_THRESHOLD && !suppressWarning;
    const needsDialog = mustConfirm || softConfirm;
    if (!needsDialog) return { action: "proceed" };

    setInitialLimit(safeLimit);
    setLimitDraft(safeLimit);
    handleOpen();

    const p = new Promise<BeforeFetchResult>((resolve) => {
      pendingResolveRef.current = resolve;
    });
    pendingPromiseRef.current = p;
    return p;
  }, [handleOpen, suppressWarning]);

  const onConfirm = useCallback(() => {
    const resolve = pendingResolveRef.current;
    if (!resolve) {
      handleClose();
      return;
    }

    let next = Math.floor(Number.isFinite(limitDraft) ? limitDraft : 0);
    if (next < 0) next = 0;
    if (next > LOGS_MAX_LIMIT) next = LOGS_MAX_LIMIT;

    setLimit(next);

    try {
      if (!suppressWarning) {
        sessionStorage.removeItem(LOGS_LIMIT_WARN_DISMISSED_KEY);
        localStorage.removeItem(LOGS_LIMIT_WARN_DISMISSED_KEY);
      } else if (persistWarning) {
        localStorage.setItem(LOGS_LIMIT_WARN_DISMISSED_KEY, "true");
        sessionStorage.removeItem(LOGS_LIMIT_WARN_DISMISSED_KEY);
      } else {
        sessionStorage.setItem(LOGS_LIMIT_WARN_DISMISSED_KEY, "true");
        localStorage.removeItem(LOGS_LIMIT_WARN_DISMISSED_KEY);
      }
    } catch (e) {
      console.error(e);
    }

    const patch = new URLSearchParams();
    patch.set("limit", String(next));
    resolve({ action: "modify", body: patch });

    // cleanup
    pendingResolveRef.current = undefined;
    pendingPromiseRef.current = null;
    handleClose();
  }, [limitDraft, setLimit, suppressWarning, persistWarning, handleClose]);

  const onCancel = useCallback(() => {
    const resolve = pendingResolveRef.current;
    if (resolve) resolve({ action: "abort" });
    pendingResolveRef.current = undefined;
    pendingPromiseRef.current = null;
    setWarningPreference(getStoredWarningPreference());
    handleClose();
  }, [handleClose]);

  const onChangeSuppressWarning = useCallback((value: boolean) => {
    setWarningPreference((current) => ({
      suppress: value,
      permanent: value && current.permanent,
    }));
  }, []);

  const onChangePersistWarning = useCallback((value: boolean) => {
    setWarningPreference((current) => ({ ...current, permanent: value }));
  }, []);

  useEffect(() => {
    return () => {
      if (pendingResolveRef.current) {
        pendingResolveRef.current({ action: "abort" });
        pendingResolveRef.current = undefined;
        pendingPromiseRef.current = null;
      }
    };
  }, []);

  const modalProps = {
    isOpen,
    initialLimit,
    limitDraft,
    setLimitDraft,
    suppressWarning,
    persistWarning,
    onChangeSuppressWarning,
    onChangePersistWarning,
    onConfirm,
    onCancel,
  };

  return {
    beforeFetch,
    modalProps,
  };
};
