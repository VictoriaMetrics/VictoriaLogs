import { FC, useCallback } from "preact/compat";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import classNames from "classnames";
import TextField from "../../../Main/TextField/TextField";
import TenantsSelectHeader from "./TenantsSelectHeader";
import { SearchIcon } from "../../../Main/Icons";
import Checkbox from "../../../Main/Checkbox/Checkbox";
import { TenantMode } from "./hooks/useTenantMode";
import "./style.scss";
import Tooltip from "../../../Main/Tooltip/Tooltip";

interface Props {
  tenants: string[];
  missingTenants: string[];
  selectedTenants: string[];
  search: {
    show: boolean;
    value: string;
    onChange: (value: string) => void;
  };
  mode: {
    value: TenantMode;
    onChange: (mode: TenantMode) => void;
  }
  onSelectAll: () => void;
  onToggleTenant: (tenant: string, mode: TenantMode) => void;
}

const TenantsSelect: FC<Props> = ({
  tenants,
  missingTenants,
  selectedTenants,
  search,
  mode,
  onSelectAll,
  onToggleTenant,
}) => {
  const { isMobile } = useDeviceDetect();
  const isMultitenantMode = mode.value === "multitenant";

  const createHandlerChange = useCallback((value: string) => () => {
    onToggleTenant(value, mode.value);
  }, [onToggleTenant, mode.value]);

  return (
    <div className="vm-tenant-input-popper">
      <TenantsSelectHeader
        tenantMode={mode.value}
        onChange={mode.onChange}
      />

      {search.show && (
        <div className="vm-tenant-input__search">
          <TextField
            autofocus
            placeholder="Search tenants..."
            value={search.value}
            onChange={search.onChange}
            type="search"
            startIcon={<SearchIcon />}
          />
        </div>
      )}

      <div
        className={classNames({
          "vm-list vm-tenant-input-list": true,
          "vm-list vm-tenant-input-list_mobile": isMobile,
        })}
      >
        {!search.value && isMultitenantMode && (
          <button
            type="button"
            className={classNames({
              "vm-tenant-input-list-item": true,
              "vm-tenant-input-list-item_all": true,
              "vm-list-item": true,
              "vm-list-item_mobile": isMobile,
              "vm-list-item_active": !selectedTenants.length
            })}
            onClick={onSelectAll}
          >
            <Checkbox
              checked={!selectedTenants.length}
              color="primary"
            />
            Select all
          </button>
        )}

        {tenants.map(id => (
          <button
            type="button"
            className={classNames({
              "vm-tenant-input-list-item": true,
              "vm-list-item": true,
              "vm-list-item_mobile": isMobile,
              "vm-list-item_active": selectedTenants.includes(id),
              "vm-tenant-input-list-item_missing": missingTenants.includes(id),
            })}
            key={id}
            onClick={createHandlerChange(id)}
          >
            {isMultitenantMode && (
              <Checkbox
                checked={selectedTenants.includes(id)}
                color="primary"
              />
            )}
            {id}
            {missingTenants.includes(id) && (
              <Tooltip title="This tenant was not found in the selected time range. It remains selected">
                <span className="vm-tenant-input-list-item__missing">unavailable</span>
              </Tooltip>
            )}
          </button>
        ))}
      </div>
    </div>
  );
};

export default TenantsSelect;
