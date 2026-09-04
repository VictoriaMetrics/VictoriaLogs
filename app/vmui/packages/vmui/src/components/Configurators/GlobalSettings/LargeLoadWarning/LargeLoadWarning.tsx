import { FC } from "preact/compat";
import Switch from "../../../Main/Switch/Switch";
import { useLocalStorageBoolean } from "../../../../hooks/useLocalStorageBoolean";

const warningDismissedKey = "LOGS_LIMIT_WARN_DISMISSED";

const LargeLoadWarning: FC = () => {
  const [warningDismissed, setWarningDismissed] = useLocalStorageBoolean(warningDismissedKey);

  return (
    <div className="vm-time-override-controller">
      <Switch
        fullWidth
        color="neutral"
        value={!warningDismissed}
        onChange={(showWarning) => setWarningDismissed(!showWarning)}
        label={<p className="vm-server-configurator__title">Show confirmation for large log loads</p>}
      />
      <div className="vm-time-override-controller__description">
        Show a confirmation before loading more than 1,000 logs.
      </div>
    </div>
  );
};

export default LargeLoadWarning;
