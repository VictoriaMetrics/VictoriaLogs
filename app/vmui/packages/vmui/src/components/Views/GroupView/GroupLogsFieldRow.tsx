import { FC, memo } from "preact/compat";
import classNames from "classnames";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import FieldRowActions from "./GroupLogsFieldRowMenu/FieldRowActions";
import { FieldStreamIcon } from "../../Main/Icons";
import Tooltip from "../../Main/Tooltip/Tooltip";

interface Props {
  field: string;
  value: string;
  isStreamField: boolean;
  hideGroupButton?: boolean;
}

const GroupLogsFieldRow: FC<Props> = ({ field, value, isStreamField, hideGroupButton = false }) => {
  const { isMobile } = useDeviceDetect();

  return (
    <tr
      className={classNames({
      "vm-group-logs-row-fields-item": true,
      "vm-group-logs-row-fields-item_mobile": isMobile
    })}
    >
      <td className="vm-group-logs-row-fields-item-controls">
        <FieldRowActions
          field={field}
          value={value}
          isStreamField={isStreamField}
          hideGroupButton={hideGroupButton}
        />
      </td>
      <td className="vm-group-logs-row-fields-item__icon">
        {isStreamField && (
          <Tooltip title="Stream field">
            <FieldStreamIcon/>
          </Tooltip>
        )}
      </td>
      <td className="vm-group-logs-row-fields-item__key">{field}</td>
      <td className="vm-group-logs-row-fields-item__value">{value}</td>
    </tr>
  );
};

export default memo(GroupLogsFieldRow);
