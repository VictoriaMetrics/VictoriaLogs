import { FC, useEffect, useRef } from "preact/compat";
import { getAppModeEnable } from "../../../../utils/app-mode";
import Button from "../../../Main/Button/Button";
import { ArrowDownIcon, RefreshIcon, RestartIcon } from "../../../Main/Icons";
import Popper from "../../../Main/Popper/Popper";
import "./style.scss";
import classNames from "classnames";
import Tooltip from "../../../Main/Tooltip/Tooltip";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import useBoolean from "../../../../hooks/useBoolean";
import { useQueryDispatch } from "../../../../state/query/QueryStateContext";
import { getMillisecondsFromDuration } from "../../../../utils/time";
import { useSearchParams } from "react-router-dom";

const delayOptions = [
  "Off",
  "1s",
  "2s",
  "5s",
  "10s",
  "30s",
  "1m",
  "5m",
  "15m",
  "30m",
  "1h",
  "2h"
];

const DEFAULT_OPTION = delayOptions[0];

const MIN_REFRESH_MS = 1000;
const MAX_REFRESH_MS = getMillisecondsFromDuration(delayOptions[delayOptions.length-1]);
const REFRESH_URL_PARAM = "refresh";

const durationToMs = (dur: string | null) => {
  if (!dur) return 0;

  try {
    return getMillisecondsFromDuration(dur);
  } catch (_e) {
    return 0;
  }
};

const isValidDelay = (ms: number) => {
  return ms >= MIN_REFRESH_MS && ms <= MAX_REFRESH_MS;
};

export const ExecutionControls: FC = () => {
  const { isMobile } = useDeviceDetect();
  const [searchParams, setSearchParams] = useSearchParams();

  const dispatch = useQueryDispatch();
  const appModeEnable = getAppModeEnable();

  const rawDelay = searchParams.get(REFRESH_URL_PARAM);
  const msDelay = durationToMs(rawDelay);
  const selectedDelay = isValidDelay(msDelay) ? rawDelay : DEFAULT_OPTION;

  const {
    value: openOptions,
    toggle: toggleOpenOptions,
    setFalse: handleCloseOptions,
  } = useBoolean(false);
  const optionsButtonRef = useRef<HTMLDivElement>(null);

  const handleChange = (dur: string) => () => {
    setSearchParams(prev => {
      const nextParams = new URLSearchParams(prev);
      const ms = durationToMs(dur);

      if (ms) {
        nextParams.set(REFRESH_URL_PARAM, `${dur}`);
      } else {
        nextParams.delete(REFRESH_URL_PARAM);
      }

      return nextParams;
    });

    handleCloseOptions();
  };

  const handleUpdate = () => {
    dispatch({ type: "RUN_QUERY" });
  };

  useEffect(() => {
    const ms = durationToMs(selectedDelay);
    let timer: number;

    if (isValidDelay(ms)) {
      timer = setInterval(() => {
        dispatch({ type: "RUN_QUERY" });
      }, ms) as unknown as number;
    }

    return () => {
      clearInterval(timer);
    };
  }, [selectedDelay]);

  return <>
    <div className="vm-execution-controls">
      <div
        className={classNames({
          "vm-execution-controls-buttons": true,
          "vm-execution-controls-buttons_mobile": isMobile,
          "vm-header-button": !appModeEnable,
        })}
      >
        {!isMobile && (
          <Tooltip title="Refresh dashboard">
            <Button
              variant="contained"
              color="primary"
              onClick={handleUpdate}
              startIcon={<RefreshIcon/>}
              aria-label="refresh dashboard"
            />
          </Tooltip>
        )}
        {isMobile ? (
          <div
            className="vm-mobile-option"
            onClick={toggleOpenOptions}
          >
            <span className="vm-mobile-option__icon"><RestartIcon/></span>
            <div className="vm-mobile-option-text">
              <span className="vm-mobile-option-text__label">Auto-refresh</span>
              <span className="vm-mobile-option-text__value">{selectedDelay}</span>
            </div>
            <span className="vm-mobile-option__arrow"><ArrowDownIcon/></span>
          </div>
        ) : (
          <Tooltip title="Auto-refresh control">
            <div ref={optionsButtonRef}>
              <Button
                variant="contained"
                color="primary"
                fullWidth
                endIcon={(
                  <div
                    className={classNames({
                      "vm-execution-controls-buttons__arrow": true,
                      "vm-execution-controls-buttons__arrow_open": openOptions,
                    })}
                  >
                    <ArrowDownIcon/>
                  </div>
                )}
                onClick={toggleOpenOptions}
              >
                {selectedDelay}
              </Button>
            </div>
          </Tooltip>
        )}
      </div>
    </div>
    <Popper
      open={openOptions}
      placement="bottom-right"
      onClose={handleCloseOptions}
      buttonRef={optionsButtonRef}
      title={isMobile ? "Auto-refresh duration" : undefined}
    >
      <div
        className={classNames({
          "vm-list": true,
          "vm-execution-controls-list": true,
          "vm-execution-controls-list_mobile": isMobile,
        })}
      >
        {delayOptions.map(d => (
          <div
            className={classNames({
              "vm-list-item": true,
              "vm-list-item_mobile": isMobile,
              "vm-list-item_active": d === selectedDelay
            })}
            key={d}
            onClick={handleChange(d)}
          >
            {d}
          </div>
        ))}
      </div>
    </Popper>
  </>;
};
