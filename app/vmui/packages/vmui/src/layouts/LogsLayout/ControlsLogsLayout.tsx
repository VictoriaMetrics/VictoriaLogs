import { FC } from "preact/compat";
import classNames from "classnames";
import GlobalSettings from "../../components/Configurators/GlobalSettings/GlobalSettings";
import { ControlsProps } from "../Header/HeaderControls/HeaderControls";
import { TimeSelector } from "../../components/Configurators/TimeRangeSettings/TimeSelector/TimeSelector";
import TenantsFields from "../../components/Configurators/GlobalSettings/TenantsConfiguration/TenantsFields";
import { ExecutionControls } from "../../components/Configurators/TimeRangeSettings/ExecutionControls/ExecutionControls";
import { useAppState } from "../../state/common/StateContext";

const ControlsLogsLayout: FC<ControlsProps> = ({ headerSetup, isMobile, closeModal }) => {
  const { tenantId } = useAppState();

  return (
    <div
      className={classNames({
        "vm-header-controls": true,
        "vm-header-controls_mobile": isMobile,
      })}
    >

      {headerSetup?.tenant && !tenantId?.disableTenantInfo && <TenantsFields/>}
      {headerSetup?.timeSelector && <TimeSelector/>}
      {headerSetup?.executionControls && <ExecutionControls
        tooltip={headerSetup?.executionControls?.tooltip}
        useAutorefresh={headerSetup?.executionControls?.useAutorefresh}
        closeModal={closeModal}
      />}
      <GlobalSettings/>
    </div>
  );
};

export default ControlsLogsLayout;
