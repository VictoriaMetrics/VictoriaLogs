import { FC } from "preact/compat";
import Switch from "../../../Main/Switch/Switch";
import { useLocalStorageBoolean } from "../../../../hooks/useLocalStorageBoolean";

const warningDismissedKey = "LOGS_LIMIT_WARN_DISMISSED";

const LargeLoadWarning: FC = () => {
  const [warningDismissed, setWarningDismissed] = useLocalStorageBoolean(warningDismissedKey);

  return (
    <Switch
      fullWidth
      color="neutral"
      value={!warningDismissed}
      onChange={(showWarning) => setWarningDismissed(!showWarning)}
      label={<p className="vm-server-configurator__title">Show confirmation for large loads</p>}
    />
  );
};

export default LargeLoadWarning;
