import { FC, useRef, useEffect } from "preact/compat";
import { useTimeDispatch } from "../../../../state/time/TimeStateContext";
import { ArrowDownIcon, QuestionIcon, StorageIcon } from "../../../Main/Icons";
import Button from "../../../Main/Button/Button";
import "./style.scss";
import "../../TimeRangeSettings/ExecutionControls/style.scss";
import classNames from "classnames";
import Popper from "../../../Main/Popper/Popper";
import { getAppModeEnable } from "../../../../utils/app-mode";
import Tooltip from "../../../Main/Tooltip/Tooltip";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import TextField from "../../../Main/TextField/TextField";
import useBoolean from "../../../../hooks/useBoolean";
import useStateSearchParams from "../../../../hooks/useStateSearchParams";
import { useSearchParams } from "react-router-dom";
import { useAppState } from "../../../../state/common/StateContext";

const TenantsFields: FC = () => {
  const { tenantId } = useAppState();
  const appModeEnable = getAppModeEnable();
  const { isMobile } = useDeviceDetect();
  const timeDispatch = useTimeDispatch();

  const [searchParams, setSearchParams] = useSearchParams();
  const [accountID, setAccountID] = useStateSearchParams(tenantId?.accountID || "0", "accountID");
  const [projectID, setProjectID] = useStateSearchParams(tenantId?.projectID || "0", "projectID");
  const formattedTenant = `${accountID}:${projectID}`;

  const buttonRef = useRef<HTMLDivElement>(null);

  const {
    value: openPopup,
    toggle: toggleOpenPopup,
    setFalse: handleClosePopup,
  } = useBoolean(false);

  const applyChanges = () => {
    searchParams.set("accountID", accountID);
    searchParams.set("projectID", projectID);
    setSearchParams(searchParams);
    handleClosePopup();
    timeDispatch({ type: "RUN_QUERY" });
  };

  const handleReset = () => {
    setAccountID(searchParams.get("accountID") || tenantId?.accountID || "0");
    setProjectID(searchParams.get("projectID") || tenantId?.projectID || "0");
  };

  useEffect(() => {
    if (openPopup) return;
    handleReset();
  }, [openPopup]);

  const isTenantStatic = !!(tenantId?.accountID || tenantId?.projectID);
  const tooltipMessage = isTenantStatic ? "Static tenant for a current user" : "Define Tenant ID if you need request to another storage";

  const getTenantLabel = () => {
    return (
      <div ref={buttonRef}>
        <div
          className="vm-mobile-option"
          onClick={isTenantStatic ? undefined : toggleOpenPopup}
        >
          <span className="vm-mobile-option__icon"><StorageIcon/></span>
          <div className="vm-mobile-option-text">
            {isMobile && (
              <span className="vm-mobile-option-text__label">Tenant ID</span>
            )}
            <span className="vm-mobile-option-text__value">{formattedTenant}</span>
          </div>
          {!isTenantStatic && (
            <span className="vm-mobile-option__arrow"><ArrowDownIcon/></span>
          )}
        </div>
      </div>
    );
  };

  return (
    <div
      className={classNames({
        "vm-tenant-input": true,
        "vm-tenant-input_disabled": isTenantStatic,
        "vm-tenant-input_mobile": isMobile,
      })}
    >
      {isMobile ? (
        getTenantLabel()
      ) : (
        <Tooltip title={tooltipMessage}>
          {isTenantStatic ? (
            getTenantLabel()
          ) : (
            <div ref={buttonRef}>
              <Button
                className={appModeEnable ? "" : "vm-header-button"}
                variant="contained"
                color="primary"
                fullWidth
                startIcon={<StorageIcon/>}
                endIcon={(
                  <div
                    className={classNames({
                      "vm-execution-controls-buttons__arrow": true,
                      "vm-execution-controls-buttons__arrow_open": openPopup,
                    })}
                  >
                    <ArrowDownIcon/>
                  </div>
                )}
                onClick={toggleOpenPopup}
              >
                {formattedTenant}
              </Button>
            </div>
          )}
        </Tooltip>
      )}
      {!isTenantStatic && (
        <Popper
          open={openPopup}
          placement="bottom-right"
          onClose={handleClosePopup}
          buttonRef={buttonRef}
          title={isMobile ? "Define Tenant ID" : undefined}
        >
          <div
            className={classNames({
              "vm-list vm-tenant-input-list": true,
              "vm-list vm-tenant-input-list_mobile": isMobile,
              "vm-tenant-input-list_inline": true,
            })}
          >
            <TextField
              autofocus
              label="accountID"
              value={accountID}
              onChange={setAccountID}
              type="number"
            />
            <TextField
              autofocus
              label="projectID"
              value={projectID}
              onChange={setProjectID}
              type="number"
            />
            <div className="vm-tenant-input-list__buttons">
              <Tooltip title="Multitenancy in VictoriaLogs documentation">
                <a
                  href="https://docs.victoriametrics.com/victorialogs/#multitenancy"
                  target="_blank"
                  rel="help noreferrer"
                >
                  <Button
                    variant="text"
                    color="gray"
                    startIcon={<QuestionIcon/>}
                  />
                </a>
              </Tooltip>
              <Button
                variant="contained"
                color="primary"
                onClick={applyChanges}
              >
                Apply
              </Button>
            </div>
          </div>
        </Popper>
      )}
    </div>
  );
};

export default TenantsFields;
