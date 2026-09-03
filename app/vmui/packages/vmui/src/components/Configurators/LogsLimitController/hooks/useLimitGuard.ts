import { useCallback, useEffect, useRef, useState } from "preact/compat";
import {
  LOGS_CONFIRM_THRESHOLD,
  LOGS_MAX_LIMIT,
  LOGS_LIMIT_WARN_DISMISSED_KEY,
} from "../../../../constants/logs";
import { BeforeFetch, BeforeFetchResult } from "../../../../pages/QueryPage/hooks/useFetchLogs";
import useBoolean from "../../../../hooks/useBoolean";

const getStoredWarningPreference = (): boolean => {
  try {
    return Boolean(localStorage.getItem(LOGS_LIMIT_WARN_DISMISSED_KEY));
  } catch {
    return false;
  }
};

type Params = {
  setLimit: (value: number) => void;
};

export const useLimitGuard = ({ setLimit }: Params) => {
  const { value: isOpen, setFalse: handleClose, setTrue: handleOpen } = useBoolean(false);

  const [initialLimit, setInitialLimit] = useState<number>(0);
  const [limitDraft, setLimitDraft] = useState<number>(0);

  const [dismissWarningDraft, setDismissWarningDraft] = useState(getStoredWarningPreference);

  const pendingResolveRef = useRef<(r: BeforeFetchResult) => void>();
  const pendingPromiseRef = useRef<Promise<BeforeFetchResult> | null>(null);

  const beforeFetch: BeforeFetch = useCallback(async (body) => {
    if (pendingPromiseRef.current) return pendingPromiseRef.current;

    const n = Number(body.get("limit") ?? 0);
    const safeLimit = Number.isFinite(n) && n >= 0 ? n : 0;

    const mustConfirm = safeLimit === 0 || safeLimit > LOGS_MAX_LIMIT;
    const warningDismissed = getStoredWarningPreference();
    const softConfirm = safeLimit > LOGS_CONFIRM_THRESHOLD && !warningDismissed;
    const needsDialog = mustConfirm || softConfirm;
    if (!needsDialog) return { action: "proceed" };

    setInitialLimit(safeLimit);
    setLimitDraft(safeLimit);
    setDismissWarningDraft(warningDismissed);
    handleOpen();

    const p = new Promise<BeforeFetchResult>((resolve) => {
      pendingResolveRef.current = resolve;
    });
    pendingPromiseRef.current = p;
    return p;
  }, [handleOpen]);

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
      if (dismissWarningDraft) {
        localStorage.setItem(LOGS_LIMIT_WARN_DISMISSED_KEY, "true");
      } else {
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
  }, [limitDraft, setLimit, dismissWarningDraft, handleClose]);

  const onCancel = useCallback(() => {
    const resolve = pendingResolveRef.current;
    if (resolve) resolve({ action: "abort" });
    pendingResolveRef.current = undefined;
    pendingPromiseRef.current = null;
    setDismissWarningDraft(getStoredWarningPreference());
    handleClose();
  }, [handleClose]);

  const onChangeSuppressWarning = useCallback((value: boolean) => {
    setDismissWarningDraft(value);
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
    suppressWarning: dismissWarningDraft,
    onChangeSuppressWarning,
    onConfirm,
    onCancel,
  };

  return {
    beforeFetch,
    modalProps,
  };
};
