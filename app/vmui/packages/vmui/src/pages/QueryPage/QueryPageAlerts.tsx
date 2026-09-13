import { FC } from "preact/compat";
import Alert from "../../components/Main/Alert/Alert";

type Props = {
  logsError?: string;
  hitsError?: string;
  timeOverridden: boolean;
}

const QueryPageAlerts: FC<Props> = ({ logsError, hitsError, timeOverridden }) => {

  if (logsError) {
    return (
      <Alert
        title="Failed to load logs"
        variant="error"
      >
        {logsError}
      </Alert>
    );
  }

  return (
    <>
      {hitsError && (
        <Alert
          title="Failed to load hits"
          variant="error"
        >
          {hitsError}
        </Alert>
      )}

      {timeOverridden && (
        <Alert
          variant="warning"
          title="Time range overridden by query filter"
        >
          Time range is overridden by the query `_time` filter. Remove `_time` from the query to use manual selection.
          Disable query time override in Settings.
        </Alert>
      )}
    </>
  );
};

export default QueryPageAlerts;
