import { useSearchParams } from "react-router-dom";
import { useCallback, useMemo } from "preact/compat";

export const useFilterViewMode = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const isEditMode = useMemo(() => searchParams.has("edit"), [searchParams.toString()]);

  const setIsEditMode = useCallback((value: boolean) => {
    const next = new URLSearchParams(searchParams);
    if (value) {
      next.set("edit", "true");
    } else {
      next.delete("edit");
    }
    setSearchParams(next, { replace: true });
  }, [searchParams, setSearchParams]);

  const toggleEditMode = useCallback(() => {
    const next = new URLSearchParams(searchParams);
    if (next.has("edit")) next.delete("edit");
    else next.set("edit", "true");
    setSearchParams(next, { replace: true });
  }, [searchParams, setSearchParams]);

  return {
    isEditMode,
    setIsEditMode,
    toggleEditMode,
  };
};
