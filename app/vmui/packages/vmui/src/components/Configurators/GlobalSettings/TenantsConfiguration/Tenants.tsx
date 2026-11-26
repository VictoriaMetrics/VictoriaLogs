import { FC, useEffect, useMemo, useRef, useState } from "preact/compat";
import { useFetchTenants } from "./hooks/useFetchTenants";
import TenantsSelect from "./TenantsSelect";
import Button from "../../../Main/Button/Button";
import classNames from "classnames";
import Tooltip from "../../../Main/Tooltip/Tooltip";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import useBoolean from "../../../../hooks/useBoolean";
import Popper from "../../../Main/Popper/Popper";
import { ArrowDownIcon, SpinnerIcon, StorageIcon } from "../../../Main/Icons";
import "./style.scss";
import "../../ExecutionControls/AutoRefreshControl/style.scss";
import { useTenants } from "../../../../hooks/useTenants";
import { useCallback } from "react";
import Alert from "../../../Main/Alert/Alert";
import { TenantMode, useTenantMode } from "./hooks/useTenantMode";
import { DEFAULT_TENANT_ID } from "../../../../constants/logs";
import { formatNumber } from "../../../../utils/number";

export type TenantType = {
  accountId: string;
  projectId: string;
}

const MAX_TENANTS_DISPLAY = 1000;
const TENANT_SEARCH_THRESHOLD = 9;

const Tenants: FC = () => {
  const { tenants, fetchTenants, cancelFetchAccountIds, isLoading, error } = useFetchTenants();
  const isReadyTenants = !error && !isLoading;
  const { tenants: selectedTenants, setTenants: setSelectedTenants } = useTenants();
  const { tenantMode, setTenantMode } = useTenantMode(tenants);
  const { isMobile } = useDeviceDetect();

  const buttonRef = useRef<HTMLDivElement>(null);
  const [search, setSearch] = useState("");

  const hasTenants = tenants.length > 0 || selectedTenants.length > 0;
  const isSingleMode = tenantMode === "single";

  const singleLabel = selectedTenants[0] || DEFAULT_TENANT_ID;
  const selectedLabel = selectedTenants.length === 1 ? selectedTenants[0] : `${selectedTenants.length} tenants`;
  const multiLabel = !selectedTenants.length ? "All" : selectedLabel;
  const label = isSingleMode ? singleLabel : multiLabel;

  const {
    value: openPopup,
    toggle: toggleOpenPopup,
    setFalse: handleClosePopup,
  } = useBoolean(false);

  const missingTenants = useMemo(() => {
    if (!selectedTenants.length) return [];
    return selectedTenants.filter((tenant) => !tenants.includes(tenant));
  }, [selectedTenants, tenants]);

  const allTenants = useMemo(() => [...missingTenants, ...tenants], [missingTenants, tenants]);
  const showSearchInput = allTenants.length > TENANT_SEARCH_THRESHOLD;

  const filteredTenants: string[] = useMemo(() => {
    if (!search) return allTenants;

    try {
      const regexp = new RegExp(search, "i");
      const found = allTenants.filter((item) => regexp.test(item));
      return found.sort((a, b) => (a.match(regexp)?.index || 0) - (b.match(regexp)?.index || 0));
    } catch (e) {
      return [];
    }
  }, [search, allTenants]);

  const limitedTenants = filteredTenants.slice(0, MAX_TENANTS_DISPLAY);

  const handleSetTenants = useCallback((nextTenants: string[]) => {
    setSelectedTenants(nextTenants, tenantMode);
  }, [setSelectedTenants, tenantMode]);

  const handleToggleTenant = useCallback((tenant: string, mode: TenantMode) => {
    const isSelected = selectedTenants.includes(tenant);

    if (mode === "single") {
      !isSelected && handleSetTenants([tenant]);
      return;
    }

    if (isSelected) {
      handleSetTenants(selectedTenants.filter((t) => tenant !== t));
      return;
    }

    handleSetTenants([...selectedTenants, tenant]);
  }, [selectedTenants, handleSetTenants]);

  const handleSelectAll = useCallback(() => {
    handleSetTenants([]);
  }, [selectedTenants, tenants, handleSetTenants]);

  useEffect(() => {
    if (openPopup) {
      void fetchTenants();
    } else {
      setSearch("");
      cancelFetchAccountIds();
    }
  }, [openPopup, fetchTenants, cancelFetchAccountIds]);

  useEffect(() => cancelFetchAccountIds, []);

  return (
    <div className="vm-tenant-input">
      <Tooltip
        title={<div>
          Define Tenant ID if you need request to another storage
          {selectedTenants.length > 1 && (
            <>
              <br/>
              <p>Selected tenants:</p>
              <ul>{selectedTenants.map((t) => <li key={t}>• {t}</li>)}</ul>
            </>
          )}
        </div>}
      >
        <div ref={buttonRef}>
          {isMobile ? (
            <div
              className="vm-mobile-option"
              onClick={toggleOpenPopup}
            >
              <span className="vm-mobile-option__icon"><StorageIcon/></span>
              <div className="vm-mobile-option-text">
                <span className="vm-mobile-option-text__label">Tenant ID</span>
                <span className="vm-mobile-option-text__value">{label}</span>
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
                    "vm-auto-refresh-control-button__arrow": true,
                    "vm-auto-refresh-control-button__arrow_open": openPopup,
                  })}
                >
                  <ArrowDownIcon/>
                </div>
              )}
              onClick={toggleOpenPopup}
            >
              {label}
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
        <TenantsSelect
          tenants={limitedTenants}
          missingTenants={missingTenants}
          selectedTenants={selectedTenants}
          search={{ show: showSearchInput, value: search, onChange: setSearch }}
          mode={{ value: tenantMode, onChange: setTenantMode }}
          onSelectAll={handleSelectAll}
          onToggleTenant={handleToggleTenant}
        />

        {isReadyTenants && !hasTenants && (
          <div className="vm-tenant-input__empty">
            No tenants found in the selected time range.
          </div>
        )}

        {limitedTenants.length < filteredTenants.length && (
          <div className="vm-tenant-input__alert">
            <Alert
              title="Too many tenants"
              variant="warning"
            >
              Showing the first {formatNumber(MAX_TENANTS_DISPLAY)} results.<br/>
              Refine your search to find a specific tenant.
            </Alert>
          </div>
        )}

        {error && (
          <div className="vm-tenant-input__alert">
            <Alert
              title="Error fetching tenants"
              variant="error"
            >
              {error}
            </Alert>
          </div>
        )}

        {isLoading && (
          <div className="vm-tenant-input__loading">
            <SpinnerIcon/> Loading tenants...
          </div>
        )}
      </Popper>
    </div>
  );
};

export default Tenants;
