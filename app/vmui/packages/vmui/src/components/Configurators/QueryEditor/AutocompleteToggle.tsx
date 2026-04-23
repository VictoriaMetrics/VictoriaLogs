import { FC } from "preact/compat";
import { useQueryDispatch, useQueryState } from "../../../state/query/QueryStateContext";
import Button from "../../Main/Button/Button";
import { AutocompleteIcon } from "../../Main/Icons";
import useDeviceDetect from "../../../hooks/useDeviceDetect";

const AutocompleteToggle: FC = () => {
  const { isMobile } = useDeviceDetect();
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
      {!isMobile && "Autocomplete: "}{autocomplete ? "On" : "Off"}
    </Button>
  );
};

export default AutocompleteToggle;
