import { FC } from "preact/compat";
import classNames from "classnames";
import GlobalSettings from "../../components/Configurators/GlobalSettings/GlobalSettings";
import { ControlsProps } from "../Header/HeaderControls/HeaderControls";
import { TimeSelector } from "../../components/Configurators/TimeRangeSettings/TimeSelector/TimeSelector";
import Tenants from "../../components/Configurators/GlobalSettings/TenantsConfiguration/Tenants";
import ShortcutKeys from "../../components/Main/ShortcutKeys/ShortcutKeys";
import { getAppModeEnable } from "../../utils/app-mode";
import Button from "../../components/Main/Button/Button";
import { KeyboardIcon, MoreIcon } from "../../components/Main/Icons";
import useBoolean from "../../hooks/useBoolean";
import Modal from "../../components/Main/Modal/Modal";
import TimeZonePreview from "../../components/Configurators/GlobalSettings/TimeZonePreview/TimeZonePreview";
import { useAppDispatch } from "../../state/common/StateContext";

const ControlsLogsLayout: FC<ControlsProps> = ({ isMobile, headerSetup }) => {
  const appModeEnable = getAppModeEnable();
  const dispatch = useAppDispatch();

  const handleOpenSettings = () => {
    dispatch({ type: "OPEN_SETTINGS" });
  };

  const {
    value: openList,
    toggle: handleToggleList,
    setFalse: handleCloseList,
  } = useBoolean(false);

  if (isMobile) {
    return (
      <>
        <div className="vm-header-controls">
          {headerSetup?.timeSelector && <TimeSelector onOpenSettings={handleOpenSettings}/>}
          <Button
            className="vm-header-button"
            startIcon={<MoreIcon/>}
            onClick={handleToggleList}
            aria-label={"controls"}
          />
        </div>
        <Modal
          title={"Controls"}
          onClose={handleCloseList}
          isOpen={openList}
          className={classNames({
            "vm-header-controls-modal": true,
            "vm-header-controls-modal_open": openList,
          })}
        >
          <div className="vm-header-controls_mobile">
            {headerSetup?.tenant && <Tenants/>}
            {headerSetup?.timeSelector && <TimeZonePreview onOpenSettings={handleOpenSettings}/>}
            <GlobalSettings/>
          </div>
        </Modal>
      </>
    );
  }

  return (
    <div className="vm-header-controls">
      {headerSetup?.tenant && <Tenants/>}
      {headerSetup?.timeSelector && <TimeSelector onOpenSettings={handleOpenSettings}/>}
      {headerSetup?.timeSelector && <TimeZonePreview onOpenSettings={handleOpenSettings}/>}
      <GlobalSettings/>
      <ShortcutKeys>
        <Button
          className={appModeEnable ? "" : "vm-header-button"}
          variant="contained"
          color="primary"
          startIcon={<KeyboardIcon/>}
        />
      </ShortcutKeys>
    </div>
  );
};

export default ControlsLogsLayout;
