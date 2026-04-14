import { FC } from "preact/compat";
import Switch from "../../../Main/Switch/Switch";
import { getFromStorage, saveToStorage } from "../../../../utils/storage";
import { useLocalStorageBoolean } from "../../../../hooks/useLocalStorageBoolean";

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
    <div>
      <div className="vm-server-configurator__title">
        Query time override
      </div>
      <Switch
        label="Override time picker with query _time filter"
        value={overrideTime}
        onChange={handleUpdateValue}
      />
    </div>
  );
};

export default QueryTimeOverride;
