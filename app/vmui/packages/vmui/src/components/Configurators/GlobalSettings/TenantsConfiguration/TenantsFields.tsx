import { FC, useCallback, useEffect, useState } from "preact/compat";
import Button from "../../../Main/Button/Button";
import classNames from "classnames";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import TextField from "../../../Main/TextField/TextField";
import { TenantType } from "./Tenants";

interface Props extends TenantType {
  accountId: string;
  projectId: string;
  onChange: (tenant: Partial<TenantType>) => void;
}

const TenantsFields: FC<Props> = ({ accountId, projectId, onChange }) => {
  const { isMobile } = useDeviceDetect();

  const [accountTmp, setAccountTmp] = useState(accountId);
  const [projectTmp, setProjectTmp] = useState(projectId);

  const applyChanges = useCallback(() => {
    onChange({
      accountId: accountTmp || accountId,
      projectId: projectTmp || projectId
    });
  }, [accountTmp, accountId, projectTmp, projectId]);

  useEffect(() => {
    setAccountTmp(accountId);
    setProjectTmp(projectId);
  }, [accountId, projectId]);

  return (
    <div
      className={classNames({
        "vm-list vm-tenant-input-list": true,
        "vm-list vm-tenant-input-list_mobile": isMobile,
      })}
    >
      <div className="vm-tenant-input-list__fields">
        <TextField
          autofocus
          label="accountID"
          value={accountTmp}
          onChange={setAccountTmp}
          type="number"
        />
        <TextField
          autofocus
          label="projectID"
          value={projectTmp}
          onChange={setProjectTmp}
          type="number"
        />
      </div>
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
        <Button
          variant="contained"
          color="primary"
          onClick={applyChanges}
        >
          Apply
        </Button>
      </div>
    </div>
  );
};

export default TenantsFields;
