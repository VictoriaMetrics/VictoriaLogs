import { FC, memo } from "preact/compat";
import classNames from "classnames";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import FieldRowActions from "./GroupLogsFieldRowMenu/FieldRowActions";

interface Props {
  field: string;
  value: string;
  hideGroupButton?: boolean;
}

const GroupLogsFieldRow: FC<Props> = ({ field, value, hideGroupButton = false }) => {
  const { isMobile } = useDeviceDetect();

  return (
    <tr
      className={classNames({
      "vm-group-logs-row-fields-item": true,
      "vm-group-logs-row-fields-item_mobile": isMobile
    })}
    >
      <td className="vm-group-logs-row-fields-item__key">{field}</td>
      <td className="vm-group-logs-row-fields-item__value">{value}</td>
      <td className="vm-group-logs-row-fields-item-controls">
        <FieldRowActions
          field={field}
          value={value}
          hideGroupButton={hideGroupButton}
        />
      </td>
    </tr>
  );
};

export default memo(GroupLogsFieldRow);
