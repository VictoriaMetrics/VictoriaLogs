import { FC, useRef } from "preact/compat";
import classNames from "classnames";
import GlobalSettings, { GlobalSettingsHandle } from "../../components/Configurators/GlobalSettings/GlobalSettings";
import { ControlsProps } from "../Header/HeaderControls/HeaderControls";
import { TimeSelector } from "../../components/Configurators/TimeRangeSettings/TimeSelector/TimeSelector";
import Tenants from "../../components/Configurators/GlobalSettings/TenantsConfiguration/Tenants";
import { ExecutionControls } from "../../components/Configurators/TimeRangeSettings/ExecutionControls/ExecutionControls";
import ShortcutKeys from "../../components/Main/ShortcutKeys/ShortcutKeys";
import { getAppModeEnable } from "../../utils/app-mode";
import Button from "../../components/Main/Button/Button";
import { KeyboardIcon, MoreIcon } from "../../components/Main/Icons";
import useBoolean from "../../hooks/useBoolean";
import Modal from "../../components/Main/Modal/Modal";

const ControlsLogsLayout: FC<ControlsProps> = ({ isMobile, headerSetup }) => {
  const appModeEnable = getAppModeEnable();
  const settingsRef = useRef<GlobalSettingsHandle>(null);

  const {
    value: openList,
    toggle: handleToggleList,
    setFalse: handleCloseList,
  } = useBoolean(false);

  if (isMobile) {
    return (
      <>
        <div className="vm-header-controls">
          {headerSetup?.timeSelector && <TimeSelector onOpenSettings={() => settingsRef.current?.open()}/>}
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
            {headerSetup?.executionControls && <ExecutionControls/>}
            <GlobalSettings ref={settingsRef}/>
          </div>
        </Modal>
      </>
    );
  }

  return (
    <div className="vm-header-controls">
      {headerSetup?.tenant && <Tenants/>}
      {headerSetup?.timeSelector && <TimeSelector onOpenSettings={() => settingsRef.current?.open()}/>}
      {headerSetup?.executionControls && <ExecutionControls/>}
      <GlobalSettings ref={settingsRef}/>
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
