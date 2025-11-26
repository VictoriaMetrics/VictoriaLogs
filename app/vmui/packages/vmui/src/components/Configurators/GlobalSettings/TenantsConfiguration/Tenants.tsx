import { FC, useRef } from "preact/compat";
import { useSearchParams } from "react-router-dom";
import { useTimeDispatch } from "../../../../state/time/TimeStateContext";
import { useFetchAccountIds } from "./hooks/useFetchAccountIds";
import TenantsSelect from "./TenantsSelect";
import TenantsFields from "./TenantsFields";
import Button from "../../../Main/Button/Button";
import classNames from "classnames";
import Tooltip from "../../../Main/Tooltip/Tooltip";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import useBoolean from "../../../../hooks/useBoolean";
import Popper from "../../../Main/Popper/Popper";
import { ArrowDownIcon, StorageIcon } from "../../../Main/Icons";
import "./style.scss";
import "../../TimeRangeSettings/ExecutionControls/style.scss";

export type TenantType = {
  accountId: string;
  projectId: string;
}

const Tenants: FC = () => {
  const { accountIds } = useFetchAccountIds();
  const { isMobile } = useDeviceDetect();
  const timeDispatch = useTimeDispatch();

  const [searchParams, setSearchParams] = useSearchParams();
  const accountId = searchParams.get("accountID") || "0";
  const projectId = searchParams.get("projectID") || "0";
  const tenantId = `${accountId}:${projectId}`;

  const buttonRef = useRef<HTMLDivElement>(null);

  const {
    value: openPopup,
    toggle: toggleOpenPopup,
    setFalse: handleClosePopup,
  } = useBoolean(false);

  const onChange = ({ accountId, projectId }: Partial<TenantType>) => {
    if (accountId) searchParams.set("accountID", accountId);
    if (projectId) searchParams.set("projectID", projectId);
    setSearchParams(searchParams);
    timeDispatch({ type: "RUN_QUERY" });
    handleClosePopup();
  };

  const childrenProps = {
    tenantId,
    accountIds,
    accountId,
    projectId,
    onChange,
  };

  return (
    <div className="vm-tenant-input">
      <Tooltip title="Define Tenant ID if you need request to another storage">
        <div ref={buttonRef}>
          {isMobile ? (
            <div
              className="vm-mobile-option"
              onClick={toggleOpenPopup}
            >
              <span className="vm-mobile-option__icon"><StorageIcon/></span>
              <div className="vm-mobile-option-text">
                <span className="vm-mobile-option-text__label">Tenant ID</span>
                <span className="vm-mobile-option-text__value">{tenantId}</span>
              </div>
              <span className="vm-mobile-option__arrow"><ArrowDownIcon/></span>
            </div>
          ) : (
            <Button
              className="vm-header-button"
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
              {tenantId}
            </Button>
          )}
        </div>
      </Tooltip>
      <Popper
        open={openPopup}
        placement="bottom-right"
        onClose={handleClosePopup}
        buttonRef={buttonRef}
        title={isMobile ? "Define Tenant ID" : undefined}
      >
        {accountIds.length ? <TenantsSelect {...childrenProps}/> : <TenantsFields {...childrenProps}/>}
      </Popper>
    </div>
  );
};

export default Tenants;
