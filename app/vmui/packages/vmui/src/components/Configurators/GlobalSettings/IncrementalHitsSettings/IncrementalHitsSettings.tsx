import { FC } from "preact/compat";
import SelectLimit from "../../../Main/Pagination/SelectLimit/SelectLimit";
import {
  INCREMENTAL_OPTIONS,
  useIncrementalTimeout
} from "../../../../pages/QueryPage/HitsPanel/hooks/useIncrementalTimeout";
import "../QueryTimeOverride/style.scss";
import "./style.scss";

const IncrementalHitsSettings: FC = () => {
  const { incrementalTimeoutLabel, setIncrementalTimeout } = useIncrementalTimeout();

  return (
    <div className="vm-time-override-controller vm-incremental-hits-settings">
      <p className="vm-server-configurator__title vm-incremental-hits-settings__title">
        Incremental hits loading
      </p>
      <div className="vm-incremental-hits-settings__select">
        <SelectLimit
          label=""
          limit={incrementalTimeoutLabel}
          options={INCREMENTAL_OPTIONS}
          onChange={setIncrementalTimeout}
        />
      </div>
      <div className="vm-time-override-controller__description vm-incremental-hits-settings__description">
        Load slow hits queries incrementally after this delay. Select Off to disable.
      </div>
    </div>
  );
};

export default IncrementalHitsSettings;
