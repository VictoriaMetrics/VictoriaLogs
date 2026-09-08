import { FC, useMemo, useState } from "preact/compat";
import { Logs } from "../../../api/types";
import "./style.scss";
import classNames from "classnames";
import GroupLogsFieldRow from "./GroupLogsFieldRow";
import { useLocalStorageBoolean } from "../../../hooks/useLocalStorageBoolean";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import TextField from "../../Main/TextField/TextField";
import { SearchIcon } from "../../Main/Icons";

interface Props {
  log: Logs;
  hideGroupButton?: boolean;
}

const GroupLogsFields: FC<Props> = ({ log, hideGroupButton }) => {
  const { isMobile } = useDeviceDetect();
  const [search, setSearch] = useState("");

  const rawEntries = useMemo(() => Object.entries(log), [log]);

  const logEntries = useMemo(() => rawEntries.filter(([key, value]) => {
    const searchLower = search.toLowerCase();
    return key.toLowerCase().includes(searchLower) || String(value).toLowerCase().includes(searchLower);
  }), [rawEntries, search]);

  const [disabledHovers] = useLocalStorageBoolean("LOGS_DISABLED_HOVERS");

  return (
    <div
      className={classNames({
        "vm-group-logs-row-fields": true,
        "vm-group-logs-row-fields_mobile": isMobile,
        "vm-group-logs-row-fields_interactive": !disabledHovers
      })}
    >
      {rawEntries.length > 8 && (
        <div className="vm-group-logs-row-fields__search-input">
          <TextField
            placeholder="Search fields or values"
            type="search"
            startIcon={<SearchIcon/>}
            value={search}
            onChange={setSearch}
          />
        </div>
      )}

      {search && logEntries.length === 0 && (
        <div className="vm-group-logs-row-fields__search-empty">
          No fields or values match your search
        </div>
      )}

      <table>
        <tbody>
          {logEntries.map(([key, value]) => (
            <GroupLogsFieldRow
              key={key}
              field={key}
              value={value}
              hideGroupButton={hideGroupButton}
            />
        ))}
        </tbody>
      </table>
    </div>
  );
};

export default GroupLogsFields;
