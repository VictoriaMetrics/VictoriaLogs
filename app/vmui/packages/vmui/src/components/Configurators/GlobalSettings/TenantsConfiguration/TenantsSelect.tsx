import { FC, useState, useMemo } from "preact/compat";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import classNames from "classnames";
import TextField from "../../../Main/TextField/TextField";
import { TenantType } from "./Tenants";
import Button from "../../../Main/Button/Button";

interface Props extends TenantType {
  accountIds: string[];
  tenantId: string;
  onChange: (tenant: Partial<TenantType>) => void;
}

const TenantsSelect: FC<Props> = ({ accountIds, tenantId, onChange }) => {
  const { isMobile } = useDeviceDetect();

  const [search, setSearch] = useState("");

  const accountIdsFiltered = useMemo(() => {
    if (!search) return accountIds;
    try {
      const regexp = new RegExp(search, "i");
      const found = accountIds.filter((item) => regexp.test(item));
      return found.sort((a,b) => (a.match(regexp)?.index || 0) - (b.match(regexp)?.index || 0));
    } catch (e) {
      return [];
    }
  }, [search, accountIds]);

  const createHandlerChange = (value: string) => () => {
    const [accountId, projectId] = value.split(":");
    onChange({ accountId, projectId });
  };

  return (
    <div
      className={classNames({
        "vm-list vm-tenant-input-list": true,
        "vm-list vm-tenant-input-list_mobile": isMobile,
      })}
    >
      <div className="vm-tenant-input-list__search">
        <TextField
          autofocus
          label="Search"
          value={search}
          onChange={setSearch}
          type="search"
        />
      </div>
      {accountIdsFiltered.map(id => (
        <div
          className={classNames({
            "vm-list-item": true,
            "vm-list-item_mobile": isMobile,
            "vm-list-item_active": id === tenantId
          })}
          key={id}
          onClick={createHandlerChange(id)}
        >
          {id}
        </div>
      ))}
      <div className="vm-tenant-input-list__buttons">
        <a
          href="https://docs.victoriametrics.com/victorialogs/#multitenancy"
          target="_blank"
          rel="help noreferrer"
        >
          <Button
            variant="text"
            color="primary"
          >
            Read more
          </Button>
        </a>
      </div>
    </div>
  );
};

export default TenantsSelect;
