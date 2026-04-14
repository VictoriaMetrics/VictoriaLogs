import { FC } from "preact/compat";
import { useQueryDispatch, useQueryState } from "../../../state/query/QueryStateContext";
import Button from "../../Main/Button/Button";
import { AutocompleteIcon } from "../../Main/Icons";

const AutocompleteToggle: FC = () => {
  const { autocomplete } = useQueryState();
  const queryDispatch = useQueryDispatch();

  const onChangeAutocomplete = () => {
    queryDispatch({ type: "TOGGLE_AUTOCOMPLETE" });
  };

  return (
    <Button
      variant="outlined"
      color={autocomplete ? "primary" : "gray"}
      onClick={onChangeAutocomplete}
      startIcon={<AutocompleteIcon/>}
    >
      Autocomplete: {autocomplete ? "On" : "Off"}
    </Button>
  );
};

export default AutocompleteToggle;
