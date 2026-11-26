import { FC } from "preact/compat";
import { LOGS_DOCS_URL } from "../../../../constants/logs";
import "./style.scss";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import classNames from "classnames";
import { ExternalLinkIcon } from "../../../Main/Icons";
import SelectLimit from "../../../Main/Pagination/SelectLimit/SelectLimit";
import Button from "../../../Main/Button/Button";
import { TenantMode } from "./hooks/useTenantMode";

type Props = {
  tenantMode: TenantMode;
  onChange: (mode: TenantMode) => void;
}

const modes = [
  {
    value: "single" as TenantMode,
    docs: `${LOGS_DOCS_URL}/#multitenancy`,
    hits: "Select one tenant to query its logs",
  },
  {
    value: "multitenant" as TenantMode,
    docs: `${LOGS_DOCS_URL}/#multitenant-querying`,
    hits: "Select multiple tenants to query their logs",
  },
];

const TenantsSelectHeader: FC<Props> = ({ tenantMode, onChange }) => {
  const { isMobile } = useDeviceDetect();
  const activeMode = modes.find(mode => mode.value === tenantMode);

  return (
    <div
      className={classNames({
        "vm-tenant-input-header": true,
        "vm-tenant-input-header_mobile": isMobile,
      })}
    >

      <div className="vm-tenant-input-header-mode">
        <SelectLimit
          label="Selection mode"
          options={modes.map((mode) => mode.value)}
          limit={tenantMode}
          onChange={onChange}
          renderOptionLabel={(v, isLabel) => (
            isLabel
              ? <span className="vm-tenant-input-header-mode-option__label">{v}</span>
              : (
                <div className="vm-tenant-input-header-mode-option">
                  <span className="vm-tenant-input-header-mode-option__label">{v}</span>
                  <span className="vm-tenant-input-header-mode-option__info">
                    {modes.find(m => v === m.value)?.hits}
                  </span>
                </div>
              )
          )}
        />
      </div>
      <Button
        as="a"
        href={activeMode?.docs || modes[0].docs}
        target="_blank"
        rel="help noreferrer"
        variant="text"
        startIcon={<ExternalLinkIcon/>}
      >
        Docs
      </Button>
    </div>
  );
};

export default TenantsSelectHeader;
