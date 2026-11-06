import { FC, useMemo } from "preact/compat";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import classNames from "classnames";
import TextField from "../../../Main/TextField/TextField";
import { TenantType } from "./Tenants";
import Button from "../../../Main/Button/Button";
import { LOGS_DOCS_URL } from "../../../../constants/logs";
import { getTenantLabel, TenantAliases } from "../../../../utils/tenant";

interface Props extends TenantType {
  accountIds: string[];
  tenantId: string;
  aliases: TenantAliases;
  search: string;
  onSearch: (value: string) => void;
  onChange: (tenant: Partial<TenantType>) => void;
}

const TenantsSelect: FC<Props> = ({ accountIds, tenantId, aliases, search, onSearch, onChange }) => {
  const { isMobile } = useDeviceDetect();

  const options = useMemo(() => accountIds.map(id => ({
    id,
    label: getTenantLabel(id, aliases),
  })), [accountIds, aliases]);

  const optionsFiltered = useMemo(() => {
    if (!search) return options;
    try {
      const regexp = new RegExp(search, "i");
      // The alias and the raw id are matched separately: searching a single
      // "<alias> <id>" haystack hides aliased tenants from anchored patterns
      // like `^0:1`, because the haystack starts with the alias.
      const matchIndex = (item: { id: string, label: string }) => Math.min(
        item.label.match(regexp)?.index ?? Infinity,
        item.id.match(regexp)?.index ?? Infinity,
      );
      return options
        .filter((item) => matchIndex(item) !== Infinity)
        .sort((a, b) => matchIndex(a) - matchIndex(b));
    } catch (e) {
      return [];
    }
  }, [search, options]);

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
          onChange={onSearch}
          type="search"
        />
      </div>
      {optionsFiltered.map(({ id, label }) => (
        <div
          className={classNames({
            "vm-list-item": true,
            "vm-tenant-input-list-item": true,
            "vm-list-item_mobile": isMobile,
            "vm-list-item_active": id === tenantId
          })}
          key={id}
          onClick={createHandlerChange(id)}
        >
          <span>{label}</span>
          {label !== id && <span className="vm-tenant-input-list-item__id">{id}</span>}
        </div>
      ))}
      <div className="vm-tenant-input-list__buttons">
        <Button
          as="a"
          href={`${LOGS_DOCS_URL}/#multitenancy`}
          target="_blank"
          rel="help noreferrer"
          variant="text"
          color="primary"
        >
          Multitenancy docs
        </Button>
      </div>
    </div>
  );
};

export default TenantsSelect;
