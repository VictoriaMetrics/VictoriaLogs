import { FC, useRef } from "preact/compat";
import { ArrowDownIcon, SettingsIcon } from "../../Main/Icons";
import Button from "../../Main/Button/Button";
import Modal from "../../Main/Modal/Modal";
import "./style.scss";
import Tooltip from "../../Main/Tooltip/Tooltip";
import { getAppModeEnable } from "../../../utils/app-mode";
import classNames from "classnames";
import Timezones from "./Timezones/Timezones";
import ThemeControl from "../ThemeControl/ThemeControl";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import useBoolean from "../../../hooks/useBoolean";

const title = "Settings";

export interface ChildComponentHandle {
  handleApply: () => void;
}

const GlobalSettings: FC = () => {
  const { isMobile } = useDeviceDetect();

  const appModeEnable = getAppModeEnable();

  const serverSettingRef = useRef<ChildComponentHandle>(null);
  const limitsSettingRef = useRef<ChildComponentHandle>(null);
  const timezoneSettingRef = useRef<ChildComponentHandle>(null);

  const {
    value: open,
    setTrue: handleOpen,
    setFalse: handleClose,
  } = useBoolean(false);

  const handleApply = () => {
    serverSettingRef.current && serverSettingRef.current.handleApply();
    limitsSettingRef.current && limitsSettingRef.current.handleApply();
    timezoneSettingRef.current && timezoneSettingRef.current.handleApply();
    handleClose();
  };

  const controls = [
    {
      show: true,
      component: <Timezones ref={timezoneSettingRef}/>
    },
    {
      show: !appModeEnable,
      component: <ThemeControl/>
    }
  ].filter(control => control.show);

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
          ariaLabel="settings"
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
          <div className="vm-server-configurator-footer">
            <Button
              color="error"
              variant="outlined"
              onClick={handleClose}
            >
              Cancel
            </Button>
            <Button
              color="primary"
              variant="contained"
              onClick={handleApply}
            >
              Apply
            </Button>
          </div>
        </div>
      </Modal>
    )}
  </>;
};

export default GlobalSettings;
