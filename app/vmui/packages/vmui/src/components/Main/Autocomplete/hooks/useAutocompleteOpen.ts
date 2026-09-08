import { useEffect } from "preact/compat";
import useBoolean from "../../../../hooks/useBoolean";

type UseAutocompleteOpenArgs = {
  value: string;
  minLength: number;
  onOpenAutocomplete?: (val: boolean) => void;
};

export const useAutocompleteOpen = ({
  value,
  minLength,
  onOpenAutocomplete,
}: UseAutocompleteOpenArgs) => {
  const getOpenState = () => value.length >= minLength;

  const {
    value: openAutocomplete,
    setValue: setOpenAutocomplete,
    setFalse: closeAutocomplete,
  } = useBoolean(getOpenState());

  useEffect(() => {
    setOpenAutocomplete(getOpenState());
  }, [value, minLength, setOpenAutocomplete]);

  useEffect(() => {
    onOpenAutocomplete?.(openAutocomplete);
  }, [openAutocomplete, onOpenAutocomplete]);

  return {
    openAutocomplete,
    closeAutocomplete,
  };
};
