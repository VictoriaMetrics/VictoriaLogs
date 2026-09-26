import { FC } from "preact/compat";
import Switch from "../../../Main/Switch/Switch";
import { getFromStorage, saveToStorage } from "../../../../utils/storage";
import { useLocalStorageBoolean } from "../../../../hooks/useLocalStorageBoolean";
import "./style.scss";

const key = "LOGS_OVERRIDE_TIME";
const defaultValue = true;

export const getOverrideValue = () => {
  const value = getFromStorage(key);
  if (!value) return defaultValue;
  return value === "true";
};

const QueryTimeOverride: FC = () => {
  const [overrideTime, setOverrideTime] = useLocalStorageBoolean(key, getOverrideValue);

  const handleUpdateValue = (value: boolean) => {
    setOverrideTime(value);
    saveToStorage(key, String(value));
  };

  return (
    <div className="vm-time-override-controller">
      <Switch
        fullWidth
        color="neutral"
        value={overrideTime}
        onChange={handleUpdateValue}
        label={<OverrideTimeLabel/>}
      />
      <div className="vm-time-override-controller__description">
        If the query contains a time filter, it overrides the selected time range.
      </div>
    </div>
  );
};

export default QueryTimeOverride;

const OverrideTimeLabel: FC = () => (
  <p className="vm-server-configurator__title">
    Override time picker with query time filter
  </p>
);
