import { FC } from "preact/compat";
import Button from "../../../Main/Button/Button";
import { useTimeState } from "../../../../state/time/TimeStateContext";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import { getUTCByTimezone } from "../../../../utils/time";
import { useMemo } from "react";
import { ArrowDownIcon, PlanetIcon } from "../../../Main/Icons";
import { getAppModeEnable } from "../../../../utils/app-mode";

type Props = {
  onOpenSettings?: () => void;
}

const TimeZonePreview: FC<Props> = ({ onOpenSettings }) => {
  const { isMobile } = useDeviceDetect();
  const appModeEnable = getAppModeEnable();

  const { timezone } = useTimeState();
  const utcOffset = useMemo(() => getUTCByTimezone(timezone), [timezone]);

  const handleOpenSettings = () => {
    onOpenSettings && onOpenSettings();
  };


  if (isMobile) {
    return (
      <button
        type="button"
        className="vm-mobile-option"
        onClick={handleOpenSettings}
      >
        <span className="vm-mobile-option__icon"><PlanetIcon/></span>
        <div className="vm-mobile-option-text">
          <span className="vm-mobile-option-text__label">Time zone</span>
          <span className="vm-mobile-option-text__value">{utcOffset}</span>
        </div>
        <span className="vm-mobile-option__arrow"><ArrowDownIcon/></span>
      </button>
    );
  }

  return (
    <Button
      className={appModeEnable ? "" : "vm-header-button"}
      onClick={handleOpenSettings}
      startIcon={<PlanetIcon/>}
    >
      {utcOffset}
    </Button>
  );
};


export default TimeZonePreview;
