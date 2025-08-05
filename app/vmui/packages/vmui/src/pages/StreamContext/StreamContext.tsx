import { useParams, useSearchParams } from "react-router-dom";
import StreamContextList from "./StreamContextList";
import { Logs } from "../../api/types";
import { LOGS_DISPLAY_FIELDS, LOGS_URL_PARAMS } from "../../constants/logs";
import { useMemo } from "react";
import "./style.scss";

const StreamContext = () => {
  const { _stream_id, _time } = useParams();

  const [searchParams] = useSearchParams();
  const displayFieldsString = searchParams.get(LOGS_URL_PARAMS.DISPLAY_FIELDS) || LOGS_DISPLAY_FIELDS;
  const displayFields = useMemo(() => displayFieldsString.split(","), [displayFieldsString]);

  if (!_stream_id || !_time) {
    return <div>Error: Missing stream ID or time.</div>;
  }

  const log: Logs = { _stream_id, _time, _msg: "", _stream: "" };

  return (
    <div className="vm-stream-context-page">
      <StreamContextList
        log={log}
        displayFields={displayFields}
      />
    </div>
  );
};

export default StreamContext;
