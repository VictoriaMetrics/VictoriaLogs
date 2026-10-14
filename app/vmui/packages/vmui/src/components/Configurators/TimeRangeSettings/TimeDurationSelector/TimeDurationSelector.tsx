import { FC } from "preact/compat";
import { relativeTimeOptions } from "../../../../utils/time";
import "./style.scss";
import classNames from "classnames";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import { RelativeTimeOption } from "../../../../types";

interface TimeDurationSelector {
  setDuration: (nextRelativeTime: RelativeTimeOption) => void;
  relativeTime: RelativeTimeOption | null;
}

const TimeDurationSelector: FC<TimeDurationSelector> = ({ relativeTime, setDuration }) => {
  const { isMobile } = useDeviceDetect();

  const createHandlerClick = (value: RelativeTimeOption) => () => {
    setDuration(value);
  };

  return (
    <div
      className={classNames({
        "vm-list": true,
        "vm-time-duration": true,
        "vm-time-duration_mobile": isMobile,
      })}
    >
      {relativeTimeOptions.map((relativeOption) => (
        <div
          className={classNames({
            "vm-list-item": true,
            "vm-list-item_mobile": isMobile,
            "vm-list-item_active": relativeOption.id === relativeTime?.id
          })}
          key={relativeOption.id}
          onClick={createHandlerClick(relativeOption)}
        >
          {relativeOption.title || relativeOption.duration}
        </div>
      ))}
    </div>
  );
};

export default TimeDurationSelector;
