import { FC, useRef } from "preact/compat";
import classNames from "classnames";
import GlobalSettings, { GlobalSettingsHandle } from "../../components/Configurators/GlobalSettings/GlobalSettings";
import { ControlsProps } from "../Header/HeaderControls/HeaderControls";
import { TimeSelector } from "../../components/Configurators/TimeRangeSettings/TimeSelector/TimeSelector";
import Tenants from "../../components/Configurators/GlobalSettings/TenantsConfiguration/Tenants";
import { ExecutionControls } from "../../components/Configurators/TimeRangeSettings/ExecutionControls/ExecutionControls";

const ControlsLogsLayout: FC<ControlsProps> = ({ isMobile, headerSetup }) => {
  const settingsRef = useRef<GlobalSettingsHandle>(null);

  return (
    <div
      className={classNames({
        "vm-header-controls": true,
        "vm-header-controls_mobile": isMobile,
      })}
    >

      {headerSetup?.tenant && <Tenants/>}
      {headerSetup?.timeSelector && <TimeSelector onOpenSettings={settingsRef?.current?.open}/>}
      {headerSetup?.executionControls &&  <ExecutionControls/>}
      <GlobalSettings ref={settingsRef}/>
    </div>
  );
};

export default ControlsLogsLayout;
