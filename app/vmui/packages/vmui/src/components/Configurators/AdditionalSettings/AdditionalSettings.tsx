import { FC, useRef } from "preact/compat";
import { useQueryDispatch, useQueryState } from "../../../state/query/QueryStateContext";
import "./style.scss";
import Switch from "../../Main/Switch/Switch";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import Popper from "../../Main/Popper/Popper";
import { TuneIcon } from "../../Main/Icons";
import Button from "../../Main/Button/Button";
import classNames from "classnames";
import useBoolean from "../../../hooks/useBoolean";
import useEventListener from "../../../hooks/useEventListener";
import Tooltip from "../../Main/Tooltip/Tooltip";
import { AUTOCOMPLETE_QUICK_KEY } from "../../Main/ShortcutKeys/constants/keyList";

const AdditionalSettingsControls: FC = () => {
  const { isMobile } = useDeviceDetect();
  const { autocomplete } = useQueryState();
  const queryDispatch = useQueryDispatch();

  const onChangeAutocomplete = () => {
    queryDispatch({ type: "TOGGLE_AUTOCOMPLETE" });
  };

  const onChangeQuickAutocomplete = () => {
    queryDispatch({ type: "SET_AUTOCOMPLETE_QUICK", payload: true });
  };

  const handleKeyDown = (e: KeyboardEvent) => {
    /** @see AUTOCOMPLETE_QUICK_KEY */
    const { code, ctrlKey, altKey } = e;
    if (code === "Space" && (ctrlKey || altKey)) {
      e.preventDefault();
      onChangeQuickAutocomplete();
    }
  };

  useEventListener("keydown", handleKeyDown);

  return (
    <div
      className={classNames({
        "vm-additional-settings": true,
        "vm-additional-settings_mobile": isMobile
      })}
    >
      <Tooltip title={<>Quick tip: {AUTOCOMPLETE_QUICK_KEY}</>}>
        <Switch
          label={"Autocomplete"}
          value={autocomplete}
          onChange={onChangeAutocomplete}
          fullWidth={isMobile}
        />
      </Tooltip>
    </div>
  );
};

const AdditionalSettings: FC = () => {
  const { isMobile } = useDeviceDetect();
  const targetRef = useRef<HTMLDivElement>(null);

  const {
    value: openList,
    toggle: handleToggleList,
    setFalse: handleCloseList,
  } = useBoolean(false);

  if (isMobile) {
    return (
      <>
        <div ref={targetRef}>
          <Button
            variant="outlined"
            startIcon={<TuneIcon/>}
            onClick={handleToggleList}
            ariaLabel="additional the query settings"
          />
        </div>
        <Popper
          open={openList}
          buttonRef={targetRef}
          placement="bottom-left"
          onClose={handleCloseList}
          title={"Query settings"}
        >
          <AdditionalSettingsControls/>
        </Popper>
      </>
    );
  }

  return <AdditionalSettingsControls/>;
};

export default AdditionalSettings;
