import { forwardRef, useImperativeHandle } from "preact/compat";
import { ArrowDownIcon, SettingsIcon } from "../../Main/Icons";
import Button from "../../Main/Button/Button";
import Modal from "../../Main/Modal/Modal";
import "./style.scss";
import Tooltip from "../../Main/Tooltip/Tooltip";
import { getAppModeEnable } from "../../../utils/app-mode";
import classNames from "classnames";
import TimezonesPicker from "./Timezones/TimezonesPicker";
import ThemeControl from "../ThemeControl/ThemeControl";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import useBoolean from "../../../hooks/useBoolean";
import QueryTimeOverride from "./QueryTimeOverride/QueryTimeOverride";
import BrowserTabController from "./BrowserTabController/BrowserTabController";
import LargeLoadWarning from "./LargeLoadWarning/LargeLoadWarning";

const title = "Settings";

export interface ChildComponentHandle {
  handleApply: () => void;
}

export interface GlobalSettingsHandle {
  open: () => void;
}

const GlobalSettings = forwardRef<GlobalSettingsHandle>((_, ref) => {
  const { isMobile } = useDeviceDetect();

  const appModeEnable = getAppModeEnable();

  const {
    value: open,
    setTrue: handleOpen,
    setFalse: handleClose,
  } = useBoolean(false);

  const controls = [
    {
      show: true,
      component: <TimezonesPicker/>
    },
    {
      show: true,
      component: <QueryTimeOverride/>
    },
    {
      show: true,
      component: <LargeLoadWarning/>
    },
    {
      show: !appModeEnable,
      component: <ThemeControl/>
    },
    {
      show: true,
      component: <BrowserTabController/>
    },
  ].filter(control => control.show);

  useImperativeHandle(ref, () => ({
    open: handleOpen,
  }));

  return <>
    {isMobile ? (
      <div
        className="vm-mobile-option"
        onClick={handleOpen}
      >
        <span className="vm-mobile-option__icon"><SettingsIcon/></span>
        <div className="vm-mobile-option-text">
          <span className="vm-mobile-option-text__label">{title}</span>
        </div>
        <span className="vm-mobile-option__arrow"><ArrowDownIcon/></span>
      </div>
    ) : (
      <Tooltip title={title}>
        <Button
          className={classNames({
            "vm-header-button": !appModeEnable
          })}
          variant="contained"
          color="primary"
          startIcon={<SettingsIcon/>}
          onClick={handleOpen}
          aria-label="settings"
        />
      </Tooltip>
    )}
    {open && (
      <Modal
        title={title}
        onClose={handleClose}
      >
        <div
          className={classNames({
            "vm-server-configurator": true,
            "vm-server-configurator_mobile": isMobile
          })}
        >
          {controls.map((control, index) => (
            <div
              className="vm-server-configurator__input"
              key={index}
            >
              {control.component}
            </div>
          ))}
        </div>
      </Modal>
    )}
  </>;
});

export default GlobalSettings;
