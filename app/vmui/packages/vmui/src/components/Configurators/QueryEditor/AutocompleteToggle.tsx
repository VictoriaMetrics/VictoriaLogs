import { FC } from "preact/compat";
import { useQueryDispatch, useQueryState } from "../../../state/query/QueryStateContext";
import Button from "../../Main/Button/Button";
import { AutocompleteIcon } from "../../Main/Icons";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import "./style.scss";

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
      {!isMobile && "Autocomplete: "}
      <span className="vm-autocomplete-status">
        <span className="vm-autocomplete-status__placeholder">Off</span>
        <span className="vm-autocomplete-status__value">
          {autocomplete ? "On" : "Off"}
        </span>
      </span>
    </Button>
  );
};

export default AutocompleteToggle;
